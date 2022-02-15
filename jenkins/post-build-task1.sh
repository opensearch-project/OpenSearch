JOB_NAME_TEMP=`basename "$JOB_NAME"`
LOG_NAME="gradle_check_${BUILD_NUMBER}.log"

sed -i -n -e "/\[$JOB_NAME_TEMP\]/,/ConsoleLogToWorkspace/p" $LOG_NAME
sed -i '/ConsoleLogToWorkspace/d' $LOG_NAME
sed -i 's/jenkins/CITOOL/g' $LOG_NAME
sed -i 's/Jenkins/CITOOL/g' $LOG_NAME
sed -i 's/workspace/workflow/g' $LOG_NAME
sed -i 's/Workspace/workflow/g' $LOG_NAME
