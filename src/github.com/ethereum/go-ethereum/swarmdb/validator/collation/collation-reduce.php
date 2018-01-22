#!/usr/bin/php
<?
error_reporting(E_ERROR);

$bandwidthcost = isset($argv[1]) ? $argv[1] : 2.71828182846;
$storagecost = isset($argv[2]) ? $argv[2] : 3.14159265359;

function output_summary($id, $tally, $bandwidthcost = 1, $storagecost = 1) 
{
    $o = new StdClass;
    $o->id = $id;
    $o->b = $tally["b"];
    $o->s = $tally["s"];
    $o->t = $tally["b"]*$bandwidthcost + $tally["s"]*$storagecost;
    echo json_encode($o)."\n";
}

$tally = array();
$previd = false;
while (($line = fgets(STDIN)) !== false) {
    $sa = explode("\t", $line);
    if ( count($sa) == 3 ) { // id\ts\tb
        $id = trim($sa[0]);
        $b = intval($sa[1]);
        $s = intval($sa[2]);
        if ( $previd && $previd != $id ) {
            output_summary($previd, $tally, $bandwidthcost, $storagecost);
            $tally = array();
        }
        $tally["b"] += $b;
        $tally["s"] += $s;
        $previd = trim($id);
    }
}
output_summary($previd, $tally, $bandwidthcost, $storagecost);
?>