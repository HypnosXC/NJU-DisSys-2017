$env:GOPATH='E:\homework\DistributedSystem\NJU-DisSys-2017'
$env:GO111MODULE='off'
go env -w GOPROXY="https://goproxy.cn,direct"
go test -run Election