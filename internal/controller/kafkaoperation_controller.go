/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	stderrors "errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/IBM/sarama"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	operationsv1alpha1 "github.io/mihkels/kafka-ops-operator/api/v1alpha1"
	"github.io/mihkels/kafka-ops-operator/internal/config"
)

const (
	retentionBytesConfig        = "retention.bytes"
	retentionMSConfig           = "retention.ms"
	requeueLongInterval         = 10 * time.Second
	kafkaAdminCloseErrorMessage = "Failed to close the admin client"
)

type RetentionSettings struct {
	bytes int64
	ms    int64
}

// KafkaOperationReconciler reconciles a KafkaOperation object
type KafkaOperationReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	Config              *config.Config
	getKafkaAdminClient func(operation *operationsv1alpha1.KafkaOperation, namespace string, logger logr.Logger) (sarama.ClusterAdmin, error)
}

// +kubebuilder:rbac:groups=operations.kafkaops.io,resources=kafkaoperations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operations.kafkaops.io,resources=kafkaoperations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operations.kafkaops.io,resources=kafkaoperations/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.4/pkg/reconcile
func (r *KafkaOperationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled, stopping reconciliation")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	operation := &operationsv1alpha1.KafkaOperation{}
	err := r.Get(ctx, req.NamespacedName, operation)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get KafkaOperation")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Store the resource version for optimistic concurrency control
	resourceVersion := operation.ResourceVersion

	// Check context again before proceeding
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled after fetching resource")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	// Handle based on current state
	var result ctrl.Result
	var reconcileErr error

	switch operation.Status.State {
	case "": // New operation
		result, reconcileErr = r.handleNewOperation(ctx, operation, logger)
	case operationsv1alpha1.OperationStatePending:
		if operation.Spec.AutoConfirm {
			result, reconcileErr = r.startOperation(ctx, operation, logger)
		} else {
			result = ctrl.Result{RequeueAfter: time.Second * 30}
		}
	case operationsv1alpha1.OperationStateWaitingForRestore,
		operationsv1alpha1.OperationStateInProgress:
		result, reconcileErr = r.processStateChange(ctx, operation, logger)
	case operationsv1alpha1.OperationStateConfirming:
		result = ctrl.Result{RequeueAfter: time.Second * 30}
	case operationsv1alpha1.OperationStateCompleted,
		operationsv1alpha1.OperationStateFailed,
		operationsv1alpha1.OperationStateCancelled:
		// No action needed for terminal states
	default:
		logger.Info("Unknown operation state", "state", operation.Status.State)
		result = ctrl.Result{RequeueAfter: time.Minute}
	}

	// Check for race conditions by verifying the resource version hasn't changed
	if reconcileErr == nil && operation.ResourceVersion != resourceVersion {
		// Resource was modified during reconciliation, refetch and retry
		logger.Info("Resource version changed during reconciliation, refetching",
			"oldVersion", resourceVersion, "newVersion", operation.ResourceVersion)

		// Refetch the resource
		refetchedOperation := &operationsv1alpha1.KafkaOperation{}
		if err := r.Get(ctx, req.NamespacedName, refetchedOperation); err != nil {
			if errors.IsNotFound(err) {
				logger.Info("Resource no longer exists")
				return ctrl.Result{}, nil
			}
			logger.Error(err, "Failed to refetch KafkaOperation")
			return ctrl.Result{RequeueAfter: time.Second * 5}, err
		}

		// If the state has changed, requeue for another reconciliation
		if refetchedOperation.Status.State != operation.Status.State {
			logger.Info("Resource state changed during reconciliation, requeueing",
				"oldState", operation.Status.State, "newState", refetchedOperation.Status.State)
			return ctrl.Result{RequeueAfter: time.Second * 5}, nil
		}
	}

	return result, reconcileErr
}

func (r *KafkaOperationReconciler) processStateChange(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled, stopping state change processing")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	logger.Info("Processing state change", "state", operation.Status.State)
	if operation.Status.State == operationsv1alpha1.OperationStateWaitingForRestore {
		waitDuration := time.Duration(operation.Spec.Timeout) * time.Second // convert int to time.Duration
		logger.Info("Waiting for restore to complete", "timeoutSeconds", waitDuration)
		if operation.Status.RetentionReducedTime != nil && time.Since(operation.Status.RetentionReducedTime.Time) < waitDuration {
			return ctrl.Result{RequeueAfter: waitDuration - time.Since(operation.Status.RetentionReducedTime.Time)}, nil
		}

		// Check context again before proceeding with restore
		select {
		case <-ctx.Done():
			logger.Info("Context cancelled before restoring topic retention")
			return ctrl.Result{}, ctx.Err()
		default:
			// Context not cancelled, proceed
		}

		return r.restoreTopicRetention(ctx, operation, getNamespace(operation), logger)
	}

	if operation.Status.State == operationsv1alpha1.OperationStateInProgress {
		select {
		case <-ctx.Done():
			logger.Info("Context cancelled before processing operation")
			return ctrl.Result{}, ctx.Err()
		default:
			// Context not cancelled, proceed
		}

		return r.processOperation(ctx, operation, logger)
	}

	return ctrl.Result{}, nil
}

func (r *KafkaOperationReconciler) handleNewOperation(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled, stopping new operation handling")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	r.initializeOperationStatus(operation)

	clusterNamespace := r.determineClusterNamespace(operation)

	admin, err := r.getKafkaAdminClient(operation, clusterNamespace, logger)
	if err != nil {
		return r.handleOperationError(ctx, operation, "FailedKafkaConnection",
			fmt.Sprintf("Failed to connect to Kafka: %v", err), logger)
	}
	defer r.closeAdminClient(admin, logger)

	// Check context again before proceeding
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled after connecting to Kafka")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	retentionSettings, err := r.fetchRetentionSettings(admin, operation.Spec.TopicName)
	if err != nil {
		return r.handleOperationError(ctx, operation, "FailedTopicConfig",
			fmt.Sprintf("Failed to get topic configuration: %v", err), logger)
	}

	r.updateRetentionStatus(operation, retentionSettings)

	// If auto-confirm is true, we'll directly move to InProgress state
	// so we don't need to update status twice
	if !operation.Spec.AutoConfirm {
		// Update status with initialized state and retention settings
		result, err := r.updateStatus(ctx, operation, logger)
		if err != nil {
			return result, err
		}
	}

	// Check context again before proceeding with auto-confirm
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled after updating status")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	if operation.Spec.AutoConfirm {
		// For auto-confirm, we'll directly move to InProgress state
		return r.startOperation(ctx, operation, logger)
	}

	return ctrl.Result{RequeueAfter: requeueLongInterval}, nil
}

func (r *KafkaOperationReconciler) initializeOperationStatus(operation *operationsv1alpha1.KafkaOperation) {
	now := metav1.Now()
	operation.Status.State = operationsv1alpha1.OperationStatePending
	operation.Status.Message = "Operation pending confirmation"

	condition := metav1.Condition{
		Type:               "Initialized",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "OperationCreated",
		Message:            "Kafka operation created and pending confirmation",
	}
	operation.Status.Conditions = append(operation.Status.Conditions, condition)
}

func (r *KafkaOperationReconciler) determineClusterNamespace(operation *operationsv1alpha1.KafkaOperation) string {
	if operation.Spec.ClusterNamespace != "" {
		return operation.Spec.ClusterNamespace
	}
	return operation.Namespace
}

func (r *KafkaOperationReconciler) closeAdminClient(admin sarama.ClusterAdmin, logger logr.Logger) {
	if err := admin.Close(); err != nil {
		logger.Error(err, kafkaAdminCloseErrorMessage)
	}
}

func (r *KafkaOperationReconciler) fetchRetentionSettings(admin sarama.ClusterAdmin, topicName string) (RetentionSettings, error) {

	settings := RetentionSettings{}
	topicConfig, err := admin.DescribeConfig(sarama.ConfigResource{
		Type: sarama.TopicResource,
		Name: topicName,
	})
	if err != nil {
		return settings, err
	}

	for _, config := range topicConfig {
		if config.Name == retentionBytesConfig {
			settings.bytes, _ = parseConfigInt64(config.Value)
		}
		if config.Name == retentionMSConfig {
			settings.ms, _ = parseConfigInt64(config.Value)
		}
	}
	return settings, nil
}

func (r *KafkaOperationReconciler) updateRetentionStatus(operation *operationsv1alpha1.KafkaOperation,
	settings RetentionSettings) {

	operation.Status.OriginalRetentionBytes = settings.bytes
	operation.Status.CurrentRetentionBytes = settings.bytes
	operation.Status.OriginalRetentionMS = settings.ms
	operation.Status.CurrentRetentionMS = settings.ms
}

func (r *KafkaOperationReconciler) startOperation(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled, stopping operation start")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	now := metav1.Now()
	operation.Status.State = operationsv1alpha1.OperationStateInProgress
	operation.Status.StartTime = &now
	operation.Status.Message = fmt.Sprintf("Starting %s operation", operation.Spec.Operation)

	// Add condition
	condition := metav1.Condition{
		Type:               "Started",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             "OperationStarted",
		Message: fmt.Sprintf("Started %s operation on topic %s",
			operation.Spec.Operation, operation.Spec.TopicName),
	}
	operation.Status.Conditions = append(operation.Status.Conditions, condition)

	// Use the updateStatus function which handles retries and conflicts
	result, err := r.updateStatus(ctx, operation, logger)
	if err != nil {
		return result, err
	}

	// Check context again before proceeding with operation
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled after updating status")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	// Continue with processing the operation
	result, err = r.processOperation(ctx, operation, logger)

	// Ensure we have a requeue after set
	if result.RequeueAfter == 0 {
		result.RequeueAfter = time.Second
	}

	return result, err
}

func (r *KafkaOperationReconciler) processOperation(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled, stopping operation processing")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	// Execute different logic based on operation type
	switch operation.Spec.Operation {
	case operationsv1alpha1.OperationResetTopic:
		return r.executeResetTopic(ctx, operation, logger)
	default:
		return r.handleOperationError(ctx, operation, "UnsupportedOperation",
			fmt.Sprintf("Unsupported operation: %s", operation.Spec.Operation), logger)
	}
}

func (r *KafkaOperationReconciler) executeResetTopic(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled, stopping reset topic execution")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	if operation.Status.CurrentRetentionBytes <= 1 { // Arbitrary small value
		logger.Info("Restoring topic retention", "topic", operation.Spec.TopicName, "originalRetention", operation.Status.OriginalRetentionBytes)
		return r.restoreTopicRetention(ctx, operation, getNamespace(operation), logger)
	}

	admin, err := r.getKafkaAdminClient(operation, getNamespace(operation), logger)
	if err != nil {
		return r.handleOperationError(ctx, operation, "FailedKafkaConnection",
			fmt.Sprintf("Failed to connect to Kafka: %v", err), logger)
	}
	defer func(admin sarama.ClusterAdmin) {
		err := admin.Close()
		if err != nil {
			logger.Error(err, kafkaAdminCloseErrorMessage)
		}
	}(admin)

	// Check context again before proceeding
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled after connecting to Kafka")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	// Set retention bytes to the value specified in the spec, or 1 (smallest possible) if not specified
	var retentionBytes int64 = 1 // Default to 1 (effectively purges the topic)
	if operation.Spec.RetentionBytes != 0 {
		retentionBytes = operation.Spec.RetentionBytes
	}
	retentionBytesStr := fmt.Sprintf("%d", retentionBytes)
	configEntries := map[string]*string{
		retentionBytesConfig: &retentionBytesStr,
	}

	// Check context again before altering config
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled before altering Kafka config")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	// Update the topic configuration
	if err = admin.AlterConfig(sarama.TopicResource, operation.Spec.TopicName, configEntries, false); err != nil {
		return r.handleOperationError(ctx, operation, "FailedRetentionUpdate",
			fmt.Sprintf("Failed to update topic retention: %v", err), logger)
	}

	// Update status
	now := metav1.Now()
	operation.Status.CurrentRetentionBytes = retentionBytes
	operation.Status.State = operationsv1alpha1.OperationStateWaitingForRestore
	operation.Status.Message = "Topic retention reduced, waiting before restore..."
	operation.Status.RetentionReducedTime = &now

	// Use the updateStatus function which handles retries and conflicts
	result, err := r.updateStatus(ctx, operation, logger)
	if err != nil {
		return result, err
	}

	// Requeue to check later if the retention period has passed
	return ctrl.Result{RequeueAfter: time.Second * 10}, nil
}

func (r *KafkaOperationReconciler) restoreTopicRetention(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, namespace string, logger logr.Logger) (ctrl.Result, error) {

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled, stopping topic retention restoration")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	// Connect to Kafka
	admin, err := r.getKafkaAdminClient(operation, namespace, logger)
	if err != nil {
		return r.handleOperationError(ctx, operation, "FailedKafkaConnection",
			fmt.Sprintf("Failed to connect to Kafka during restoration: %v", err), logger)
	}
	defer func(admin sarama.ClusterAdmin) {
		err := admin.Close()
		if err != nil {
			logger.Error(err, kafkaAdminCloseErrorMessage)
		}
	}(admin)

	// Check context again before proceeding
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled after connecting to Kafka")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	// Determine what retention to restore to
	configEntries := make(map[string]*string)

	// Handle retention bytes
	restoreRetentionBytes := operation.Status.OriginalRetentionBytes
	if operation.Spec.RestoreRetentionBytes != 0 {
		restoreRetentionBytes = operation.Spec.RestoreRetentionBytes
	}
	retentionBytesStr := fmt.Sprintf("%d", restoreRetentionBytes)
	configEntries[retentionBytesConfig] = &retentionBytesStr

	// Handle retention MS if specified
	if operation.Spec.RestoreRetentionMS != 0 || operation.Status.OriginalRetentionMS != 0 {
		restoreRetentionMS := operation.Status.OriginalRetentionMS
		if operation.Spec.RestoreRetentionMS != 0 {
			restoreRetentionMS = operation.Spec.RestoreRetentionMS
		}
		retentionMSStr := fmt.Sprintf("%d", restoreRetentionMS)
		configEntries["retention.ms"] = &retentionMSStr
	}

	// Check context again before altering config
	select {
	case <-ctx.Done():
		logger.Info("Context cancelled before altering Kafka config")
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	// Update the topic configuration
	if err = admin.AlterConfig(sarama.TopicResource, operation.Spec.TopicName, configEntries, false); err != nil {
		return r.handleOperationError(ctx, operation, "FailedRetentionRestore",
			fmt.Sprintf("Failed to restore topic retention: %v", err), logger)
	}

	// Complete the operation
	logger.Info("Restoring topic retention", "topic", operation.Spec.TopicName, "originalRetention", operation.Status.OriginalRetentionBytes)
	now := metav1.Now()
	operation.Status.State = operationsv1alpha1.OperationStateCompleted
	operation.Status.CompletionTime = &now
	operation.Status.CurrentRetentionBytes = restoreRetentionBytes
	operation.Status.Message = "Topic reset completed successfully, retention restored"

	// Add condition
	addCondition(operation, "Completed", "OperationCompleted",
		"Topic reset operation completed successfully")

	return r.updateStatus(ctx, operation, logger)
}

func (r *KafkaOperationReconciler) handleOperationError(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, reason, message string, log logr.Logger) (ctrl.Result, error) {
	// Check if context is cancelled
	select {
	case <-ctx.Done():
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	log.Error(stderrors.New(message), "Operation failed", "reason", reason)

	// Update status
	now := metav1.Now()
	operation.Status.State = operationsv1alpha1.OperationStateFailed
	operation.Status.CompletionTime = &now
	operation.Status.Message = message

	// Add condition
	condition := metav1.Condition{
		Type:               "Failed",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}
	operation.Status.Conditions = append(operation.Status.Conditions, condition)

	// Use the updateStatus function which handles retries and conflicts
	return r.updateStatus(ctx, operation, log)
}

func (r *KafkaOperationReconciler) loadKafkaTLSConfig(
	ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation,
	namespace string,
	kafkaConfig *sarama.Config,
) error {
	secretName := ""
	if operation.Spec.Username != "" {
		secretName = operation.Spec.Username
	} else {
		secretName = r.Config.KafkaAuthUser
		if secretName == "" {
			return fmt.Errorf("KAFKA_AUTH_USER environment variable is not set")
		}
	}

	var userSecret corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: secretName}, &userSecret); err != nil {
		return fmt.Errorf("failed to get Kafka user secret %s in namespace %s: %w", secretName, namespace, err)
	}

	caCert := userSecret.Data["ca.crt"]
	userCert := userSecret.Data["user.crt"]
	userKey := userSecret.Data["user.key"]
	if len(caCert) == 0 || len(userCert) == 0 || len(userKey) == 0 {
		return fmt.Errorf("kafka user secret %s in namespace %s is missing required fields", secretName, namespace)
	}

	cert, err := tls.X509KeyPair(userCert, userKey)
	if err != nil {
		return fmt.Errorf("failed to parse TLS certificate and key from Kafka user secret %s in namespace %s: %w", secretName, namespace, err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCert) {
		return fmt.Errorf("failed to append CA certificate from Kafka user secret %s in namespace %s", secretName, namespace)
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caPool,
		MinVersion:         tls.VersionTLS12, // Ensure TLS 1.2 or higher
		InsecureSkipVerify: true,
	}
	kafkaConfig.Net.TLS.Enable = true
	kafkaConfig.Net.TLS.Config = tlsConfig

	return nil
}

func (r *KafkaOperationReconciler) defaultGetKafkaAdminClient(
	operation *operationsv1alpha1.KafkaOperation,
	namespace string, logger logr.Logger) (sarama.ClusterAdmin, error) {
	// Create Sarama configuration
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V3_8_1_0 // Use appropriate Kafka version

	clusterName := ""
	if operation.Spec.ClusterName != "" {
		clusterName = operation.Spec.ClusterName
	} else {
		clusterName = r.Config.KafkaClusterName
		if clusterName == "" {
			return nil, fmt.Errorf("KAFKA_CLUSTER_NAME environment variable is not set")
		}
	}

	port := r.Config.KafkaPort
	if operation.Spec.UseTls {
		port = "9093"
	} else if port == "" {
		port = "9092"
	}

	// Get bootstrap servers from Strimzi CR or Secret
	bootstrapServers := []string{fmt.Sprintf("%s-kafka-bootstrap.%s:%s",
		clusterName,
		namespace,
		port),
	}

	logger.Info("Bootstrap servers", "servers", bootstrapServers)

	if r.Config.KafkaAuth || operation.Spec.UseTls {
		logger.Info("Loading Kafka TLS configuration")
		if err := r.loadKafkaTLSConfig(context.Background(), operation, namespace, kafkaConfig); err != nil {
			logger.Error(err, "Failed to load Kafka TLS configuration")
			return nil, err
		}
		logger.Info("Kafka TLS configuration loaded successfully")
	}

	// Create the admin client
	admin, err := sarama.NewClusterAdmin(bootstrapServers, kafkaConfig)
	if err != nil {
		logger.Error(err, "Failed to create Kafka admin client")
		return nil, err
	}

	return admin, nil
}

// Helper function to get namespace
func getNamespace(operation *operationsv1alpha1.KafkaOperation) string {
	if operation.Spec.ClusterNamespace != "" {
		return operation.Spec.ClusterNamespace
	}
	return operation.Namespace
}

// Helper function to add a condition
func addCondition(operation *operationsv1alpha1.KafkaOperation, condType, reason, message string) {
	now := metav1.Now()
	condition := metav1.Condition{
		Type:               condType,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            message,
	}
	operation.Status.Conditions = append(operation.Status.Conditions, condition)
}

// Helper function for status updates
func (r *KafkaOperationReconciler) updateStatus(ctx context.Context,
	operation *operationsv1alpha1.KafkaOperation, logger logr.Logger) (ctrl.Result, error) {

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		return ctrl.Result{}, ctx.Err()
	default:
		// Context not cancelled, proceed
	}

	logger.Info("Updating KafkaOperation status", "state", operation.Status.State)

	// Retry logic for handling resource version conflicts
	maxRetries := 3
	retryCount := 0

	for retryCount < maxRetries {
		err := r.Status().Update(ctx, operation)
		if err == nil {
			// If we had to retry due to conflicts, return with requeue to ensure proper state handling
			if retryCount > 0 {
				return ctrl.Result{RequeueAfter: time.Second * 10}, nil
			}
			return ctrl.Result{}, nil
		}

		if errors.IsConflict(err) {
			// Resource version conflict, retry
			retryCount++
			logger.Info("Resource version conflict, retrying", "attempt", retryCount)

			// Refetch the resource to get the latest version
			var refetchedOperation operationsv1alpha1.KafkaOperation
			if err := r.Get(ctx, client.ObjectKey{Namespace: operation.Namespace, Name: operation.Name}, &refetchedOperation); err != nil {
				logger.Error(err, "Failed to refetch KafkaOperation")
				return ctrl.Result{RequeueAfter: time.Second * 5}, err
			}

			// Update the resource version and preserve our status changes
			refetchedOperation.Status = operation.Status
			operation = &refetchedOperation

			// Check context again before retrying
			select {
			case <-ctx.Done():
				return ctrl.Result{}, ctx.Err()
			default:
				// Context not cancelled, continue with retry
			}
		} else {
			// Not a conflict error, return
			logger.Error(err, "Failed to update KafkaOperation status")
			return ctrl.Result{RequeueAfter: time.Second * 5}, err
		}
	}

	// Max retries exceeded
	logger.Error(fmt.Errorf("max retries exceeded"), "Failed to update KafkaOperation status after multiple attempts")
	return ctrl.Result{RequeueAfter: time.Second * 10}, fmt.Errorf("failed to update status after %d attempts", maxRetries)
}

// SetupWithManager sets up the controller with the Manager.
func (r *KafkaOperationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Initialize the getKafkaAdminClient field with the default implementation
	if r.getKafkaAdminClient == nil {
		r.getKafkaAdminClient = r.defaultGetKafkaAdminClient
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&operationsv1alpha1.KafkaOperation{}).
		Named("kafkaoperation").
		Complete(r)
}

func parseConfigInt64(value string) (int64, error) {
	var result int64
	_, err := fmt.Sscanf(value, "%d", &result)
	if err != nil {
		return 0, fmt.Errorf("failed to parse value '%s' to int64: %w", value, err)
	}
	return result, nil
}
