#!/usr/bin/php
<?
error_reporting(E_ERROR);

function validate_chunk($ip, $port, $chunkID)
{
    return(true);

    // fetch the chunk from the node (within a specific timeout limit, up to N times)
    $url = "http://$ip:$port/chunk/$chunkID";
    $t0 = microtime(true);
    $session = curl_init($url);
    curl_setopt($session, CURLOPT_HEADER, false);
    curl_setopt($session, CURLOPT_HTTPHEADER, array( 'Connection: Keep-Alive', 'Keep-Alive: 300' ));
    curl_setopt($session, CURLOPT_RETURNTRANSFER, true);
    $res = trim(curl_exec($session));

    // 1. check the timestamp
    return(true);
}

function output_summary($chunkID, $chunkinfo)
{
    $tot = 0;
    $minReplication = 1;
    $maxReplication = 0;
    if ( isset($chunkinfo["buyers"]) ) {
        $buyers = array();
        $farmers = array();
        foreach ( $chunkinfo["buyers"] as $buyerID => $info ) {
            if ( $info->minReplication < $minReplication ) {
                $minReplication = $info->minReplication;
            }
            if ( $info->maxReplication > $maxReplication ) {
                $maxReplication = $info->maxReplication;
            }
            $buyers[] = $buyerID; // DESIGN: what to do if there is more than one buyer?  with different minReplication / maxReplication?
            unset($info->ip);
            unset($info->port);
            unset($info->chunkID);
        }
        $valid = 0;
        foreach ( $chunkinfo["farmers"] as $farmerID => $info) {
            // validate that the farmer has the chunk
            if ( validate_chunk($chunkID, $info->ip, $info->port) ) {
                $info->valid = 1;
                $valid++;
                $farmers[] = $farmerID;
            } else {
                $info->valid = 0;
            }
            unset($info->ip);
            unset($info->port);
            unset($info->chunkID);
        }
        if ( $valid < $minReplication ) {
            // DESIGN: how should insurer should push this chunk into more farmers hands when this is detected?
        }
        if ( $valid > $maxReplication ) {
            // DESIGN: can only reward that many but need to choose them deterministically so validators end up with the same answer
        }
        $out = new StdClass;
        $out->chunkID = $chunkID;
        $out->buyers = $buyers;
        $out->minReplication = $minReplication;   
        $out->maxReplication = $maxReplication;
        $out->farmers = $farmers; // valid = count($farmers) so not including it
        echo json_encode($out)."\n";
    } else {
        // DESIGN: if farmers are making claims for data that buyers don't care about, what should be done?
    }
}

$validator_publickey = isset($argv[1]) ? $argv[1] : "validator_publickey"; 
$total_chunks = 0;
$total_valid = 0;
$previd = false;
while (($line = fgets(STDIN)) !== false) {
    $sa = explode("\t", $line);
    if ( count($sa) == 2 ) { // chunkID\tfarmerIDdata or buyerIDdata
        $chunkID = trim($sa[0]);
        $info = json_decode($sa[1]);
        if ( $previd && $previd != $chunkID ) {
            output_summary($previd, $chunkinfo);
            $chunkinfo = array();
        }
        if ( isset($info->buyer) ) {
            $chunkinfo["buyers"][$info->buyer] = $info;
        } else if ( isset($info->farmer) ) {
            $chunkinfo["farmers"][$info->farmer] = $info;
        }
        
        $previd = trim($chunkID);
    }
}
output_summary($previd, $chunkinfo);
?>