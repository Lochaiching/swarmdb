#!/usr/bin/php
<?
error_reporting(E_ERROR);

$previd = false;
while (($line = fgets(STDIN)) !== false) {
    $sa = explode("\t", $line);
    if ( count($sa) == 2 ) { 
        $id = trim($sa[0]);
        if ( $previd && $previd != $id ) {
            output_summary($previd, $tally);
            $tally = array();
        }
        $previd = trim($id);
        $s = json_decode($sa[1]);
        if ( isset($s->farmer) ) {
            // add this farmer ONLY if farmer can return a valid SMASH proof (could be done in mapper, but only need to do this if buyer record exists)
            // and give immediate feedback to the farmer
            $tally["farmers"][] = $s->farmer;
        } else if ( isset($s->buyer) ) {
            $tally["buyers"][] = $s->buyer;
            $tally["rep"] = $s->rep;
            $tally["renewable"] = $s->renewable;
            $tally["chunkBD"] = $s->chunkBD;
            $tally["smash"] = $s->smash;
            $tally["sig"] = $s->sig;
        }
    }
}
output_summary($previd, $tally);

function output_summary($id, $tally)
{
    $out = new StdClass;
    $out->id = $id;
    $out->buyers = $tally["buyers"];
    $out->farmers = $tally["farmers"];
    $out->rep = $tally["rep"];
    $out->renewable = $tally["renewable"];
    $out->chunkBD = $tally["chunkBD"];
    $out->smash = $tally["smash"];
    $out->sig = $tally["sig"];
    echo json_encode($out)."\n";

 }
?>