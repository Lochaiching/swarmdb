#!/usr/bin/php
<?
error_reporting(E_ERROR);

$tally = array();
while (($line = fgets(STDIN)) !== false) {
    $s = json_decode(trim($line), false);
    $chunkID = $s->chunkID;
    if ( isset($s->buyer) ) {
        echo $chunkID."\t".json_encode($s)."\n";
    } else if ( isset($s->farmer) ) {
        echo $chunkID."\t".json_encode($s)."\n";
    } else {
        print_r($s);
    }
}
?>