#!/usr/bin/env python3
"""
Script to split install.yaml into Helm chart templates and CRDs.
Preserves existing Helm templating and improvements.
"""

import os
import re
import sys
from pathlib import Path

def setup_directories(chart_dir):
    """Create necessary directories."""
    output_dir = chart_dir / 'templates'
    crds_dir = chart_dir / 'crds'

    output_dir.mkdir(parents=True, exist_ok=True)
    crds_dir.mkdir(parents=True, exist_ok=True)

    return output_dir, crds_dir

def find_install_yaml():
    """Find the install.yaml file relative to the script location."""
    script_dir = Path(__file__).parent / '../dist/install.yaml'
    if script_dir.exists():
        return script_dir

    return None

def extract_metadata(doc):
    """Extract kind, name from YAML document."""
    kind_match = re.search(r'^kind:\s*(\S+)', doc, re.MULTILINE)
    name_match = re.search(r'^metadata:\n(?:  .*\n)*  name:\s*(\S+)', doc, re.MULTILINE)

    kind = kind_match.group(1) if kind_match else 'unknown'
    name = name_match.group(1) if name_match else 'noname'

    return kind, name

def helmify_content(doc):
    """Add Helm templating to YAML content."""
    kind_match = re.search(r'^kind:\s*(\S+)', doc, re.MULTILINE)
    if not kind_match:
        return doc

    kind = kind_match.group(1)

    # Don't helmify CRDs
    if kind == 'CustomResourceDefinition':
        return doc

    # Replace static namespace with Helm template
    doc = re.sub(
        r'  namespace: kafka-ops-operator-system',
        '  namespace: {{ .Release.Namespace }}',
        doc
    )

    # Replace static names with templated names (preserve specific patterns)
    doc = re.sub(
        r'  name: kafka-ops-operator-([a-zA-Z0-9-]+)',
        r'  name: {{ include "kafka-ops-operator.fullname" . }}-\1',
        doc
    )

    # Replace static image with Helm template for Deployment
    if kind == 'Deployment':
        doc = re.sub(
            r'        image: mihkels/kafka-ops-operator:[\d.]+',
            '        image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"',
            doc
        )

    # Add Helm labels using helper method
    if 'metadata:' in doc and 'labels:' not in doc:
        # Add labels section if it doesn't exist
        doc = re.sub(
            r'(metadata:\n)',
            r'\1  labels:\n    {{- include "kafka-ops-operator.labels" . | nindent 4 }}\n',
            doc
        )
    elif 'labels:' in doc:
        # Replace existing labels with Helm template
        doc = re.sub(
            r'(metadata:\n(?: {2}.*\n)*?) {2}labels:\n(?: {4}.*\n)*',
            r'\1  labels:\n    {{- include "kafka-ops-operator.labels" . | nindent 4 }}\n',
            doc
        )

    return doc

def generate_filename(idx, kind, name):
    """Generate consistent filename for resources using simple incremental numbering."""
    prefix = f'{idx:03d}'
    return f'{prefix}-{kind}-{name}.yaml'

def preserve_existing_helmification(filepath, new_content):
    """Update content with latest from install.yaml while preserving existing Helm templating for non-RBAC resources."""
    kind_match = re.search(r'^kind:\s*(\S+)', new_content, re.MULTILINE)
    kind = kind_match.group(1) if kind_match else 'unknown'

    # Force update RBAC resources to ensure they have latest permissions
    rbac_kinds = ['Role', 'ClusterRole', 'RoleBinding', 'ClusterRoleBinding']
    if kind in rbac_kinds:
        print(f"  Force updating RBAC resource {filepath.name}")
        return helmify_content(new_content)

    # For non-RBAC resources, preserve existing Helm templating if file exists
    if not filepath.exists():
        print(f"  Creating new file {filepath.name}")
        return helmify_content(new_content)

    with open(filepath, 'r') as f:
        existing_content = f.read()

    # If existing file has Helm templating, preserve it
    if '{{' in existing_content and '}}' in existing_content:
        print(f"  Preserving existing Helm templating in {filepath.name}")
        return existing_content

    # Otherwise, apply standard helmification
    print(f"  Updating {filepath.name} with Helm templating")
    return helmify_content(new_content)

def extract_image_version_from_install_yaml(content):
    """Extract the image version from install.yaml Deployment."""
    # Look for the image line in the Deployment
    image_match = re.search(r'image: mihkels/kafka-ops-operator:([\d.]+)', content)
    if image_match:
        return image_match.group(1)
    return None

def update_chart_yaml(chart_dir, app_version):
    """Update Chart.yaml with the latest appVersion."""
    chart_yaml_path = chart_dir / 'Chart.yaml'

    if not chart_yaml_path.exists():
        print(f"Warning: {chart_yaml_path} not found, skipping Chart.yaml update")
        return False

    with open(chart_yaml_path, 'r') as f:
        chart_content = f.read()

    # Update appVersion
    updated_content = re.sub(
        r'^appVersion:\s*["\']?[\d.]+["\']?',
        f'appVersion: "{app_version}"',
        chart_content,
        flags=re.MULTILINE
    )

    if updated_content != chart_content:
        with open(chart_yaml_path, 'w') as f:
            f.write(updated_content)
        print(f"Updated Chart.yaml appVersion to {app_version}")
        return True
    else:
        print(f"Chart.yaml appVersion already up to date ({app_version})")
        return False

def process_install_yaml(chart_name='kafka-ops-operator'):
    """Process install.yaml and update Helm charts."""
    script_dir = Path(__file__).parent
    chart_dir = script_dir / chart_name

    if not chart_dir.exists():
        print(f"Error: Chart directory {chart_dir} not found.")
        return False

    print(f"Working with chart directory: {chart_dir}")

    input_file = find_install_yaml()
    if not input_file:
        print("Error: install.yaml not found. Run 'make build-installer' first.")
        return False

    print(f"Found install.yaml at: {input_file}")

    with open(input_file, 'r') as f:
        content = f.read()

    # Extract image version and update Chart.yaml
    image_version = extract_image_version_from_install_yaml(content)
    if image_version:
        update_chart_yaml(chart_dir, image_version)
    else:
        print("Warning: Could not extract image version from install.yaml")

    output_dir, crds_dir = setup_directories(chart_dir)

    docs = [doc.strip() for doc in content.split('---') if doc.strip()]

    for idx, doc in enumerate(docs):
        kind, name = extract_metadata(doc)

        # Determine output directory and filename
        directory = crds_dir if kind == 'CustomResourceDefinition' else output_dir
        filename = generate_filename(idx, kind, name)
        filepath = directory / filename

        # Always update with latest content and apply Helm templating
        final_content = preserve_existing_helmification(filepath, doc)

        # Ensure content ends with exactly one newline
        final_content = final_content.rstrip('\n') + '\n'

        # Write the file
        with open(filepath, 'w') as out:
            out.write(final_content)

        action = "Updated" if filepath.exists() else "Created"
        print(f'{action} {filename} in {directory.relative_to(chart_dir)}')

    return True

if __name__ == "__main__":
    # Support command line argument for chart name
    chart_name = sys.argv[1] if len(sys.argv) > 1 else 'kafka-ops-operator'

    if process_install_yaml(chart_name):
        print("\nHelm chart templates updated successfully!")
        print("Existing Helm templating has been preserved.")
        print("\nNext steps:")
        print(f"1. Review the generated templates in {chart_name}/")
        print(f"2. Test with: helm template kafka-ops-operator {chart_name}/ --debug")
        print(f"3. Validate with: helm lint {chart_name}/")
    else:
        sys.exit(1)