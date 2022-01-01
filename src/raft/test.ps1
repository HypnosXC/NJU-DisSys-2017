$env:GOPATH='E:\homework\DistributedSystem\NJU-DisSys-2017'
$env:GO111MODULE='off'
go env -w GOPROXY="https://goproxy.cn,direct"
$range = 1..1000
$cnt = 0
$count = $range.Count           
For($i=0; $i -lt $count; $i++) {           
    go test -v 
    if (!$?) {
        echo fail at test $i
        exit
    }     
}
echo fail is $cnt/100