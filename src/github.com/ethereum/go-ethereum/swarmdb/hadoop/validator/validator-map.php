#!/usr/bin/php
<?
error_reporting(E_ERROR);
function validate_chunk($ip, $port, $chunkID)
{
    return(true);

    // fetch the chunk from the node (within a specific timeout limit, up to N times)
    $url = "http://$ip:$port/swarmdb://$chunkID";
    $t0 = microtime(true);
    $session = curl_init($url);
    curl_setopt($session, CURLOPT_HEADER, false);
    curl_setopt($session, CURLOPT_HTTPHEADER, array( 'Connection: Keep-Alive', 'Keep-Alive: 300' ));
    curl_setopt($session, CURLOPT_RETURNTRANSFER, true);
    $res = trim(curl_exec($session));

    // 1. check the timestamp
    return(true);
}

function flush_tally($tally) 
{
    foreach ( $tally as $farmerID => $arr) {
        echo $farmerID."\t".$arr["chunks"]."\t".$arr["valid"]."\n";
    }
}

$thresh = 100;  // after testing this many, output a tally
$tally = array();
while (($line = fgets(STDIN)) !== false) {
    $s = json_decode(trim($line), false);
    // this contains a claimed chunk that needs validation 
    $chunkID = $s->chunkID; // chunk that needs validation
    $farmerID = $s->farmer; // public key of the farmer (the address can be recovered from this)
    $ip = $s->ip; // ip + port of farmer
    $port = $s->port; 
    $tally[$farmerID]["chunks"]++;
    if ( validate_chunk($ip, $port, $chunkID) ) {
        $tally[$farmerID]["valid"]++;
    }
    if ( $chunks > $thresh ) {
        flush_tally($tally);
    }
    $chunks++;
}
flush_tally($tally);
?>