#!/usr/bin/php
<?
error_reporting(E_ERROR);
function insure_chunk($ip, $port, $chunkID, $minReplication = 3, $maxReplication = 5)
{
    return(true);

    // fetch the chunk from SWARM, check that its replication is meeting min and not exceeding max too much
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
    foreach ( $tally as $buyerID => $arr) {
        echo $buyerID."\t".$arr["chunks"]."\t".$arr["valid"]."\n";
    }
}

$thresh = 100;  // after testing this many, output a tally
$tally = array();
while (($line = fgets(STDIN)) !== false) {
    $s = json_decode(trim($line), false);
    // this contains a claimed chunk that needs validation 
    $chunkID = $s->chunkID; // chunk that needs validation
    $buyerID = $s->buyer; // public key of the farmer (the address can be recovered from this)
    $ip = $s->ip; // ip + port of SWARMDB node
    $port = $s->port; 
    $minReplication = $s->minReplication; 
    $maxReplication = $s->maxReplication; 
    if ( strlen($buyerID) > 0 ) {
        $tally[$buyerID]["chunks"]++;
        if ( insure_chunk($ip, $port, $chunkID, $minReplication, $maxReplication) ) {
            $tally[$buyerID]["valid"]++;
        }
        if ( $chunks > $thresh ) {
            flush_tally($tally);
        }
        $chunks++;
    }
}
flush_tally($tally);
?>