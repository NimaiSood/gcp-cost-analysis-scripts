#!/bin/bash

echo "--- Identifying Projects without User-Defined Labels ---"
echo ""

# Get a list of all project IDs accessible by your credentials
# You might want to filter this list further if you have many projects
PROJECT_IDS=$(gcloud projects list --format="value(projectId)")

if [ -z "${PROJECT_IDS}" ]; then
  echo "No projects found or an error occurred."
  exit 1
fi

echo "Total projects to check: $(echo "${PROJECT_IDS}" | wc -l | tr -d ' ')"
echo "------------------------------------------------------------------"

NO_LABEL_PROJECTS=()

for PROJECT_ID in ${PROJECT_IDS}; do
  echo "Checking project: ${PROJECT_ID}"
  
  # Fetch project details and check if the 'labels' field exists and is not empty.
  # We use gcloud projects describe and parse its output.
  # If 'labels:' appears in the YAML output and has any entries, it means labels exist.
  # The grep -q makes the check silent (no output unless an error).
  if ! gcloud projects describe "${PROJECT_ID}" --format="yaml" 2>/dev/null | grep -q "^labels: [[:space:]]*[^[:space:]]"; then
    # This regex "^labels: [[:space:]]*[^[:space:]]" checks for 'labels:' followed by
    # at least one non-whitespace character, indicating actual labels are present.
    # If the grep returns false (no labels found), then add to the list.
    NO_LABEL_PROJECTS+=("${PROJECT_ID}")
    echo "  ➡️ Project '${PROJECT_ID}' **DOES NOT** have any user-defined labels."
  else
    echo "  ✅ Project '${PROJECT_ID}' has labels. Skipping."
  fi
done

echo ""
echo "--- Report: Projects without User-Defined Labels ---"
echo "----------------------------------------------------"

if [ ${#NO_LABEL_PROJECTS[@]} -eq 0 ]; then
  echo "🎉 No projects found without user-defined labels."
else
  echo "The following projects lack user-defined labels:"
  for PROJECT in "${NO_LABEL_PROJECTS[@]}"; do
    echo "  - ${PROJECT}"
  done
fi

echo ""
echo "--- Script Finished ---"