#!/usr/bin/php
<?
error_reporting(E_ERROR);

while (($line = fgets(STDIN)) !== false) {
    $s = json_decode(trim($line), false);
    if ( isset($s->id) && isset($s->remote) ) {
        $id = $s->id;
        echo "$id\t".$s->b."\n";
    }
}

?>