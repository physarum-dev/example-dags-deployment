TEST_TEAM_NAME = "quickstart"  # Setting this to a team in your `teams.json` will configure jobs to run with a staging JAR
GROUP_BY_BATCH_CONCURRENCY = 300  # Increase as required if many group_bys per team causing DAGs to fall behind
JOIN_CONCURRENCY = 100  # Increase as required if large Joins causing DAGs to fall behind
ZIPLINE_PATH = "sample"
time_parts = ["ds", "ts",
              "hr"]  # The list of time-based partition column names used in your warehouse. These are used to set up partition sensors in DAGs.
