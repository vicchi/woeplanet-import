<?php

$src = '"Woe_id"        "Rep_id"        "Data_Version"';
$nospace = preg_replace("/[[:blank:]]+/", " ", $src);
$noquote = preg_replace('/"/', "", $nospace);
var_dump($nospace);
var_dump($noquote);
$fields = explode(" ", $noquote);
var_dump($fields);
?>
