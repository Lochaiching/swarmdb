#!/usr/bin/php
<?
error_reporting(E_ERROR);

$previd = false;
while (($line = fgets(STDIN)) !== false) {
    $sa = explode("\t", trim($line));
    if ( count($sa) == 2 ) { // id\tb
        $id = trim($sa[0]);
        $b = intval($sa[1]);
        if ( $previd && ( $previd != $id ) ) {
            output_summary($previd, $total);
            $total = 0;
        }
        $previd = $id;
        $total += $b;
    }
}
output_summary($previd, $total);

function output_summary($id, $total)
{
    $out = new StdClass;
    $out->id = $id;
    $out->b = $total;
    echo json_encode($out)."\n";
}
?>