#!/usr/bin/php
<?
error_reporting(E_ERROR);

while (($line = fgets(STDIN)) !== false) {
    $s = json_decode(trim($line), false);
    if ( isset($s->id) && isset($s->remote) ) {
        $id = $s->id;
        $remote = $s->remote;
        $receipt = $s->receipt;
        // TODO: check for valid signature in receipt matching id
        if ( $id > $remote ) {
            echo "$remote:$id\t".$s->b."\tR\n";
        } else {
            echo "$id:$remote\t".$s->b."\tI\n";
        }
    }
}

?>