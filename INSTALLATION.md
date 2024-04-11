Don't Enable RAM SWAP (Test it out)

Multiple copies cause socket error

Max out network and nlimit

`sudo nano /etc/sysctl.conf`

[//]: # File Content
```
fs.file-max = 100000

net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.ipv4.tcp_fin_timeout = 15
net.ipv4.ip_local_port_range = 1024 65535

net.core.default_qdisc=fq
net.ipv4.tcp_congestion_control=bbr
net.core.somaxconn = 4096

net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_tw_recycle = 1
```

`sudo nano /etc/security/limits.conf`

[//]: #File limiter
```
* soft nofile 1048576
* hard nofile 1048576
```

[//]: # For the systemdl service to work. link the libonnxruntime file
```shell
sudo ln -s /home/ubuntu/pipeless/pipeless/target/release/libonnxruntime.so.1.16.0 /usr/local/lib/libonnxruntime.so.1.16.0
sudo ldconfig
```

[//]: #Driver Permission
Set permission for nvidia driver incase it fails
```shell
sudo chmod a+rw /dev/nvidia0
```