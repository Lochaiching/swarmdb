#!/usr/bin/php
<?
set_time_limit(0);

$d = isset($argv[1])? $argv[1] : date("Y/m/d", time() - 86400);
$t = date("Ymd-Hm", time());

$prev = "validator";
$prev2 = "insurer";
$job = "aggregator";

// these should be swarmdb urls but we'll put swarmdb logs here in gcloud buckets 
$input = "gs://wolk_swarmdb/$prev/$d";
$input2 = "gs://wolk_swarmdb/$prev2/$d";
$output = "gs://wolk_swarmdb/$job/$d";

$project = "crosschannel-1307";
$bucket = "wolk_hadoop";
$pri = "HIGH";
$queuename = "heavy";
$dev = "/sourabh";
$nreduces = 0;
$cluster = $job."-".$t;
$basedir = "/var/www/vhosts$dev/swarm.wolk.com/src/github.com/ethereum/go-ethereum/swarmdb";
$mapper = "$basedir/hadoop/$job/$job-map.php";
$reducer = "$basedir/hadoop/$job/$job-reduce.php";
$gsmapper = "gs://$bucket/$job/$job-mapper.php";
$gsreducer = "gs://$bucket/$job/$job-reduce.php";
$cmd = array();
$cmd[] = "gsutil cp $mapper gs://$bucket/$job/$job-map.php";
$cmd[] = "gsutil cp $reducer gs://$bucket/$job/$job-reduce.php";
$cmd[] = "gcloud dataproc clusters create $cluster --zone us-central1-b --master-machine-type n1-standard-4 --master-boot-disk-size 100 --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 100 --project $project --initialization-actions 'gs://startup_scripts_us/scripts/dataproc/startup-dataproc.sh'";
$cmd[] = "gcloud dataproc jobs submit hadoop --cluster $cluster --jar file:///usr/lib/hadoop-mapreduce/hadoop-streaming.jar --files $gsmapper,$gsreducer -D mapred.job.name=$job/$d -D mapred.job.queue.name=light -mapper $job-map.php -reducer $job-reduce.php   -input $input -input $input2 -output $output -numReduceTasks 1";
$cmd[] = "gcloud -q dataproc clusters delete $cluster";
// submit the transaction via Web3 JS
$cmd[] = "gsutil cat $output/* | node < submittx.js > /var/log/$job/$d";

foreach ($cmd as $c) {
    echo "$c\n";
    //exec($c);
}
?>
