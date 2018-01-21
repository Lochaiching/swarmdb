#!/usr/bin/php
<?
error_reporting(E_ERROR);

$previd = false;
while (($line = fgets(STDIN)) !== false) {
    $sa = explode("\t", trim($line));
    if ( count($sa) == 3 ) { // buyerID\tchunks\tvalid
        $pair = trim($sa[0]);
        $b = intval($sa[1]);
        $first = trim($sa[2]);
        if ( $previd && ( $previd != $pair ) ) {
            output_summary($previd, $tally);
            $tally = array();
        }
        $previd = $pair;
        $p = explode(":", $previd);
        if ( $first == "R" ) {
            $tally[] = array($p[1], $p[0], $b);
        } else if ( $first == "I" ) {
            $tally[] = array($p[0], $p[1], $b);
        }
    }
}
output_summary($pair, $tally);

function output_summary($pair, $tally)
{
    if ( count($tally) == 2 ) {
        foreach ($tally as $t) {
            $out = new StdClass;
            $out->id = $t[0];
            $out->remote = $t[1];
            $out->b = $t[2];
            echo json_encode($out)."\n";
        }
    } else if ( count($tally) == 1 ) {
        // unmatched!  TODO: report to the first
    }
}
?>