#!/usr/bin/php
<?
error_reporting(E_ERROR);

$validator_publickey = isset($argv[1]) ? $argv[1] : "validator_publickey"; 

$total_chunks = 0;
$total_valid = 0;
$previd = false;
while (($line = fgets(STDIN)) !== false) {
    $sa = explode("\t", $line);
    if ( count($sa) == 3 ) { // farmerID\tchunks\tvalid
        $farmerID = trim($sa[0]);
        $chunks = intval($sa[1]);
        $valid = intval($sa[2]);
        if ( $previd && $previd != $farmerID ) {
            output_summary($validator_publickey, $previd, $total_chunks, $total_valid);
            $total_chunks = 0;
            $total_valid = 0;
        }
        $total_chunks += $chunks;
        $total_valid += $valid;
        $previd = trim($farmerID);
    }
}
output_summary($validator_publickey, $previd, $total_chunks, $total_valid);

function output_summary($validator_publickey, $id, $total_chunks, $total_valid) 
{
    $out = new StdClass;
    $out->validator = $validator_publickey;
    $out->farmer = $id;
    $out->chunks = $total_chunks;
    $out->valid = $total_valid;
    echo json_encode($out)."\n";
}
?>