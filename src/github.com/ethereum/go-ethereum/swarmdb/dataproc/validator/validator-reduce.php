#!/usr/bin/php
<?
error_reporting(E_ERROR);

$validator_publickey = isset($argv[1]) ? $argv[1] : "validator_publickey"; 

$total_b = 0;
$total_f = 0;
$previd = false;
while (($line = fgets(STDIN)) !== false) {
    $sa = explode("\t", $line);
    if ( count($sa) == 3 ) { // farmerID\tchunks\tvalid
        $id = trim($sa[0]);
        $b = intval($sa[1]);
        $f = intval($sa[2]);
        if ( $previd && $previd != $farmerID ) {
            output_summary($validator_publickey, $previd, $total_b, $total_f);
            $total_b = 0;
            $total_f = 0;
        }
        $total_b += $b;
        $total_f += $f;
        $previd = trim($id);
    }
}
output_summary($validator_publickey, $previd, $total_b, $total_f);

function output_summary($validator_publickey, $id, $total_b, $total_f) 
{
    $out = new StdClass;
    $out->validator = $validator_publickey;
    $out->id = $id;
    $out->b = $total_b;
    $out->f = $total_f;
    echo json_encode($out)."\n";
}
?>