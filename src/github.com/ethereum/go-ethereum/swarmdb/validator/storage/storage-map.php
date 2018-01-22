#!/usr/bin/php
<?
error_reporting(E_ERROR);

function flush_tally($tally) 
{
    foreach ($tally as $id => $c) {
        echo "$id\t".intval($c["b"])."\t".intval($c["f"])."\n";
    }
}

function selected_buyer($buyers) 
{
    if (count($buyers) > 0 ) {
        return($buyers[0]); // TODO: 
    }
    return(false);
}

function selected_farmers($farmers, $buyer, $rep) 
{
    if (count($farmers) > 0 ) {
        return($farmers); // TODO: 
    }
    return(false);
}

$thresh = 100;  // after testing this many, output a tally
$tally = array();
while (($line = fgets(STDIN)) !== false) {
    $s = json_decode(trim($line), false);
    // this contains a claimed chunk that needs validation 
    $chunkID = $s->id; // chunk that needs validation
    if ( $buyer = selected_buyer($s->buyers) ) {
        if ( $farmers = selected_farmers($s->farmers, $buyer, $s->rep) ) {
            if ( count($farmers) < 3 ) {
                // let the buyer know we have TOO FEW farmers - major problem
            }
            foreach ($farmers as $farmerID) {
                $tally[$buyer]["b"]++;
                $tally[$farmerID]["f"]++;
            }
        } else {
            // let the buyer know we have NO farmers -  major problem
            echo "NO FARMER\n";
        }
    } else {
        // let all the farmers know
        echo "NO BUYER\n";
    }
    if ( $c++ % $thresh == 0 ) {
        flush_tally($tally);
    }
}
flush_tally($tally);
?>