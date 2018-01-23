#!/usr/bin/php
<?
error_reporting(E_ERROR);

function output_summary($id, $tally)
{
    $o = new StdClass;
    $o->id = $id;
    $o->sb = $tally["b"];
    $o->sf = $tally["f"];
    $o->s = $tally["f"] - $tally["b"];
    echo json_encode($o)."\n";
}

$previd = false;  // after testing this many, output a tally
$tally = array();
while (($line = fgets(STDIN)) !== false) {
    $sa = explode("\t", $line);
    if ( count($sa) == 3 ) { 
        $id = trim($sa[0]);
        if ( $previd && $previd != $id ) {
            output_summary($previd, $tally);
            $tally = array();
        }
        $previd = trim($id);
        $tally["b"] += $sa[1];
        $tally["f"] += $sa[2];
    }
    
}
if ( $previd ) {
    output_summary($id, $tally);
}
?>