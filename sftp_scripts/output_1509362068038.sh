local_temp_folder="/tmp/staging"
remote_log_path="/home/mapr/test"
remote_machine="172.16.243.116"
remote_user="mapr"
hdfs_folder_path="test/"
home_path="/home/"$remote_user
# Make local temp directory to store the remote files.
echo "Checking if tmp staging folder exists?"
echo $local_temp_folder
if [ -d $local_temp_folder ]
then
echo "Temp staging Directory already  exists. Hence removing."
rm -rf $local_temp_folder
fi

# Creating Temp staging local directory
echo "Creating temp local directory"
mkdir $local_temp_folder
echo "temp local directory created"

# Make secure copy of file from remote server.

echo "Securely copying data from Remote Hosts"
scp -r $remote_user@$remote_machine:$remote_log_path/* $local_temp_folder
echo "Copying data complete ..."

#Push Files into HDFS
echo "Pushing files into HDFS"
hadoop fs -put $local_temp_folder/* $hdfs_folder_path
echo "Files pushed into HDFS"

# Move Remote file to Done directory
echo "Moving Remote files to Done directory"
ssh $remote_user@$remote_machine << 'ENDSSH'

remote_user="mapr"
hdfs_folder_path="test/"
home_path="/home/"$remote_user
remote_log_path="/home/mapr/test"

echo "Homepath : $home_path"
if  [ ! -d "$home_path/Done" ]
then
echo "Creating Done Directory"
mkdir "$home_path/Done"
fi

mv $remote_log_path/* $home_path/Done

echo "Remote Files moved to Done directory"
exit;
ENDSSH