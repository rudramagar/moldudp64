# How to test Multicast Locally?

Listen
```sh
./sock_test 239.1.1.1 8000 0.0.0.0
INFO : Joined : 239.1.1.1
Listening...
recv bytes=6
```

Sent using python
```python
python3 - <<'PY'
import socket
mcast_group = ("239.1.1.1", 8000)
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
s.sendto(b"hello\n", mcast_group)
print("sent")
PY
```

Build in centos7 and higher version
```sh
# Rocky9
make

# CentOS7
scl enable devtoolset-7 -- make
```
