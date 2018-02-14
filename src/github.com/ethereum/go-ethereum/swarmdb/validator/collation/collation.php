#!/usr/bin/php
<?
set_time_limit(0);

$d = isset($argv[1])? $argv[1] : "test";
$t = date("Ymd", time());

$prev = "bandwidth";
$prev2 = "storage";
$job = "collation";

// these should be swarmdb urls but we'll put swarmdb logs here

$project = "crosschannel-1307";
$bucket = "wolk_validator";
$input = "gs://$bucket/$prev/$d";
$input2 = "gs://$bucket/$prev2/$d";
$output = "gs://$bucket/$job/$d/$t";
$pri = "HIGH";
$queuename = "heavy";
$dev = "/sourabh";
$nreduces = 0;
$cluster = "validator-".$t;
$basedir = "/var/www/vhosts$dev/swarm.wolk.com/src/github.com/ethereum/go-ethereum/swarmdb";
$mapper = "$basedir/validator/$job/$job-map.php";
$reducer = "$basedir/validator/$job/$job-reduce.php";
$gsmapper = "gs://$bucket/$job/$job-map.php";
$gsreducer = "gs://$bucket/$job/$job-reduce.php";
$cmd = array();
$cmd[] = "gsutil cp $mapper gs://$bucket/$job/$job-map.php";
$cmd[] = "gsutil cp $reducer gs://$bucket/$job/$job-reduce.php";

// create cluster
// $cmd[] = "gcloud dataproc clusters create $cluster --zone us-central1-b --master-machine-type n1-standard-1 --master-boot-disk-size 10 --num-workers 2   --worker-machine-type n1-standard-1 --worker-boot-disk-size 10 --project $project --initialization-actions 'gs://startup_scripts_us/scripts/dataproc/startup-dataproc-go-2018.sh'";

// submit job
$cmd[] = "gcloud dataproc jobs submit hadoop --cluster $cluster  --jar file:///usr/lib/hadoop-mapreduce/hadoop-streaming.jar -- --files $gsmapper,$gsreducer  -D mapreduce.job.name=$job-$t -mapper $job-map.php -reducer $job-reduce.php  -input $input -input $input2 -output $output -numReduceTasks 1";

// delete
// $cmd[] = "gcloud -q dataproc clusters delete $cluster";

foreach ($cmd as $c) {
    echo "$c\n";
    //exec($c);
}
?>
