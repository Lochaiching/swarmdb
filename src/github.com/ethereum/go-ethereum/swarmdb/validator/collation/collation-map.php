#!/usr/bin/php
<?
error_reporting(E_ERROR);

$tally = array();
while (($line = fgets(STDIN)) !== false) {
    $s = json_decode(trim($line), false);
    if ( isset($s->id) ) {
        echo $s->id."\t".intval($s->s)."\t".intval($s->b)."\n";
    }
}
?>