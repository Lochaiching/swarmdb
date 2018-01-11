#!/usr/bin/php
<?
error_reporting(E_ERROR);

function flush_tally($tally) 
{
    foreach ( $tally as $id => $arr) {
        echo $id."\t".intval($arr["b"])."\t".intval($arr["f"])."\n";
    }
}

$thresh = 5000;  // after testing this many, output a tally
$tally = array();
while (($line = fgets(STDIN)) !== false) {
    $s = json_decode(trim($line), false);
    // this contains a claimed chunk that needs validation 
    $chunkID = $s->chunkID; // chunk that needs validation
    if ( isset($s->buyers) ) {
        asort($s->buyers); // TODO: need salting with chunkID here  so farmers chosen deterministically across validators
        foreach ($s->buyers as $buyerID) {
            $tally[$buyerID]["b"]++;
            break; // only taking first one 
        }
    }
    if ( isset($s->farmers) ) {
        asort($s->farmers); // TODO: need salting with chunkID here so farmers chosen deterministically across validators
        foreach ($s->farmers as $farmerID) {
            if ( $cnt > $s->maxReplication ) {
                break;
            } else {
                $tally[$farmerID]["f"]++;
            }
            $cnt++;
        }
    }
    if ( $chunks > $thresh ) {
        flush_tally($tally);
    }
    $chunks++;
}
flush_tally($tally);
?>