#!/usr/bin/php
<?
error_reporting(E_ERROR);

$insurer_publickey = isset($argv[1]) ? $argv[1] : "insurer_publickey"; 

$total_chunks = 0;
$total_valid = 0;
$previd = false;
while (($line = fgets(STDIN)) !== false) {
    $sa = explode("\t", $line);
    if ( count($sa) == 3 ) { // buyerID\tchunks\tvalid
        $buyerID = trim($sa[0]);
        $chunks = intval($sa[1]);
        $valid = intval($sa[2]);
        if ( $previd && ( $previd != $buyerID ) ) {
            output_summary($insurer_publickey, $previd, $total_chunks, $total_valid);
            $total_chunks = 0;
            $total_valid = 0;
        }
        $total_chunks += $chunks;
        $total_valid += $valid;
        $previd = trim($buyerID);
    }
}
output_summary($insurer_publickey, $previd, $total_chunks, $total_valid);

function output_summary($insurer_publickey, $id, $total_chunks, $total_valid) 
{
    $out = new StdClass;
    $out->insurer = $insurer_publickey;
    $out->buyer = $id;
    $out->chunks = $total_chunks;
    $out->valid = $total_valid;
    echo json_encode($out)."\n";
}
?>