#!/bin/bash
set -e

# --- CONFIGURATION ---
BASE_DIR="$HOME/Documents/GitHub/mapreduce"
PACKAGE="edu.univ.haifa.bigdata"

# --- HELPER FUNCTION ---
# This function handles the logic for a single question.
# It expects 2 arguments: 
#   $1 = Question Number (e.g., 1)
#   $2 = Java Class Name (e.g., ScreenTimeAnalysis)
run_question() {
    Q_NUM=$1
    CLASS_NAME=$2
    
    echo "=================================================="
    echo "   STARTING QUESTION $Q_NUM ($CLASS_NAME) "
    echo "=================================================="

    # 1. Build
    echo "--- Building Question $Q_NUM ---"
    cd "$BASE_DIR/Question$Q_NUM/"
    mvn clean install

    # 2. Copy JAR
    echo "--- Copying JAR to Docker ---"
    # Note: Assuming the jar is always named questionX.jar based on your pom.xml
    docker cp "target/question$Q_NUM.jar" namenode:/question$Q_NUM.jar

    # 3. Cleanup Old Output (Fail-safe with || true)
    echo "--- Cleaning HDFS Output Directory ---"
    docker exec namenode hdfs dfs -rm -r -skipTrash output_question$Q_NUM || true

    # 4. Run Hadoop Job
    echo "--- Running MapReduce Job ---"
    docker exec namenode hadoop jar /question$Q_NUM.jar $PACKAGE.$CLASS_NAME screentime_analysis.csv output_question$Q_NUM

    # 5. Show Results
    echo "--- RESULTS FOR QUESTION $Q_NUM: ---"
    docker exec namenode hdfs dfs -cat output_question$Q_NUM/part-r-00000
    echo ""
}

# --- MENU SYSTEM ---
echo "Which question would you like to run?"
echo "1) ScreenTimeAnalysis"
echo "2) AvgNotificationsDriver"
echo "3) DailyAppLaunchDriver"
echo "4) NotificationsUsageDriver"
echo "5) DayOfWeekLaunchDriver"
echo "all) Run EVERYTHING (1-5)"
echo ""
read -p "Enter your choice (1-5 or all): " choice

# --- EXECUTION LOGIC ---
case $choice in
    1)
        run_question 1 "ScreenTimeAnalysis"
        ;;
    2)
        run_question 2 "AvgNotificationsDriver"
        ;;
    3)
        run_question 3 "DailyAppLaunchDriver"
        ;;
    4)
        run_question 4 "NotificationsUsageDriver"
        ;;
    5)
        run_question 5 "DayOfWeekLaunchDriver"
        ;;
    all)
        run_question 1 "ScreenTimeAnalysis"
        run_question 2 "AvgNotificationsDriver"
        run_question 3 "DailyAppLaunchDriver"
        run_question 4 "NotificationsUsageDriver"
        run_question 5 "DayOfWeekLaunchDriver"
        ;;
    *)
        echo "Invalid option. Please run the script again and choose 1-5 or 'all'."
        exit 1
        ;;
esac

echo "=================================================="
echo "   JOB FINISHED "
echo "=================================================="
