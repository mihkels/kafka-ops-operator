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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type OperationType string

const (
	OperationResetTopic OperationType = "ResetTopic"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// KafkaOperationSpec defines the desired state of KafkaOperation.
type KafkaOperationSpec struct {
	// Operation type to be performed
	// +kubebuilder:validation:Required
	Operation OperationType `json:"operation"`

	// Reference to a Strimzi KafkaTopic resource
	// +kubebuilder:validation:Required
	TopicName string `json:"topicName"`

	// Reference to a Kafka cluster
	// +kubebuilder:validation:Required
	ClusterName string `json:"clusterName,omitempty"`

	// Namespace of the Kafka cluster
	// +optional
	ClusterNamespace string `json:"clusterNamespace,omitempty"`

	// RetentionBytes to set for the operation
	// For Reset operations: set temporarily to clear messages, then restore
	// +optional
	RetentionBytes int64 `json:"retentionBytes,omitempty"`

	// RetentionMS to set for the operation (milliseconds)
	// +optional
	RetentionMS int64 `json:"retentionMs,omitempty"`

	// RestoreRetentionBytes is the retention bytes value to restore after reset
	// +optional
	RestoreRetentionBytes int64 `json:"restoreRetentionBytes,omitempty"`

	// RestoreRetentionMS is the retention ms value to restore after reset
	// +optional
	RestoreRetentionMS int64 `json:"restoreRetentionMs,omitempty"`

	// Whether to automatically confirm the operation
	// If false, operation requires manual confirmation for execution
	// +kubebuilder:default=false
	AutoConfirm bool `json:"autoConfirm,omitempty"`

	// Timeout specifies how long to wait (in seconds) after reducing retention before restoring it
	// +kubebuilder:default=30
	Timeout int `json:"timeoutSeconds,omitempty"`

	// Username for Kafka client
	// +kubebuilder:default=""
	Username string `json:"username,omitempty"`

	// Whether to use TLS
	// +kubebuilder:default=false
	UseTls bool `json:"useTls,omitempty"`
}

type OperationState string

const (
	OperationStatePending           OperationState = "Pending"
	OperationStateConfirming        OperationState = "Confirming"
	OperationStateInProgress        OperationState = "InProgress"
	OperationStateCompleted         OperationState = "Completed"
	OperationStateFailed            OperationState = "Failed"
	OperationStateCancelled         OperationState = "Cancelled"
	OperationStateWaitingForRestore OperationState = "WaitingForRestore"
)

// KafkaOperationStatus defines the observed state of KafkaOperation.
type KafkaOperationStatus struct {
	// The current state of the operation
	// +optional
	State OperationState `json:"state,omitempty"`

	// Original retention settings before any changes
	// +optional
	OriginalRetentionBytes int64 `json:"originalRetentionBytes,omitempty"`
	OriginalRetentionMS    int64 `json:"originalRetentionMs,omitempty"`

	// Current retention settings
	// +optional
	CurrentRetentionBytes int64 `json:"currentRetentionBytes,omitempty"`
	CurrentRetentionMS    int64 `json:"currentRetentionMs,omitempty"`

	// Start time of the operation
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// Completion time of the operation
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Message describing the current state
	// +optional
	Message string `json:"message,omitempty"`

	// Detailed conditions of the operation
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Timeout for the operation in seconds
	// +optional
	RetentionReducedTime *metav1.Time `json:"retentionReducedTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// KafkaOperation is the Schema for the kafkaoperations API.
type KafkaOperation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaOperationSpec   `json:"spec,omitempty"`
	Status KafkaOperationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// KafkaOperationList contains a list of KafkaOperation.
type KafkaOperationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaOperation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaOperation{}, &KafkaOperationList{})
}
