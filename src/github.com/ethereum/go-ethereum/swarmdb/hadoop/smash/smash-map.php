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

$thresh = 5000;  // after testing this many, output a tally
while (($line = fgets(STDIN)) !== false) {
    $s = json_decode(trim($line), false);
    if ( isset($s->chunkID) && isset($s->farmer) ) {
        if ( validate_chunk($s->farmer, $chunkID) ) {
            $chunkID = $s->chunkID; 
            echo $chunkID."\t".json_encode($s)."\n";
        }
    } else {
        print_r($s);
    }
}
?>