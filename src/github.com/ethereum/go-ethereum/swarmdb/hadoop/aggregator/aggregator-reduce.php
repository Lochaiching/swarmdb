#!/usr/bin/php
<?
error_reporting(E_ERROR);

function consensus($inp) 
{
    // do at least 2/3 have the same answer?
    $tot = 0;
    foreach ($inp as $judge => $output) {
        $tally[$output]++;
        $tot++;
    }
    arsort($tally);
    foreach ($tally as $res => $cnt) {
        if ( $cnt >= $tot*2/3 ) {
            return $cnt;
        }
        break;
    }
    return(false);
}

function flush_tally($buyers, $farmers) 
{
    $tot = 0;
    foreach ( $buyers as $buyerID => $insurers) {
        if ( $output = consensus($insurers) ) {
            $o = new StdClass;
            $o->buyerID = $buyerID;
            $o->tally = $output;
            $tot += $output;
            echo json_encode($o)."\n";
        }
    }
    // TODO: this needs to be scaled downwards
    foreach ( $farmers as $farmerID => $validators) {
        if ( $output = consensus($insurers) ) {
            $o = new StdClass;
            $o->farmerID = $buyerID;
            $o->tally = $output;
            echo json_encode($o)."\n";
        }
    }
}

$thresh = 100;  // after testing this many, output a tally
$tally = array();
while (($line = fgets(STDIN)) !== false) {
    $s = json_decode(trim($line), false);
    // this contains a claimed chunk that needs validation 
    if ( isset($s->buyer) ) {
        $buyers[$s->buyer][$s->insurer] = $s->valid;
    } else if ( isset($s->farmer) ) {
        $farmers[$s->farmer][$s->validator] = $s->valid;
    }
}
flush_tally($buyers, $farmers);
?>