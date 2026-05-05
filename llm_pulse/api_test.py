import requests
import json

# Get your workspace URL and token
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

ENDPOINT_NAME = "llm-pulse-cost-forecast"

# Build a sample input matching your feature columns
sample_input = {
    "inputs": [{
        "day_of_week":         2,
        "day_of_month":        15,
        "month":               3,
        "week_of_year":        11,
        "is_weekend":          0,
        "cost_lag_1d":         0.25,
        "cost_lag_3d":         0.22,
        "cost_lag_7d":         0.28,
        "cost_lag_14d":        0.24,
        "cost_lag_30d":        0.20,
        "rolling_mean_7d":     0.26,
        "rolling_mean_14d":    0.25,
        "rolling_std_7d":      0.03,
        "total_calls":         45.0,
        "rolling_7d_avg_cost": 0.26,
        "team_encoded":        0
    }]
}

# Call the endpoint
response = requests.post(
    url   = f"https://{WORKSPACE_URL}/serving-endpoints/{ENDPOINT_NAME}/invocations",
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type":  "application/json"
    },
    data = json.dumps(sample_input)
)

print(f"Status: {response.status_code}")
print(f"Predicted cost: {response.json()}")