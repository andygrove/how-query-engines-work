apt update
apt -y install musl musl-dev musl-tools

# Because we're using musl, we need to compile OpenSSL from source and statically link against it
curl -o openssl-1.0.2s.tar.gz https://www.openssl.org/source/openssl-1.0.2s.tar.gz
tar xzf openssl-1.0.2s.tar.gz
cd openssl-1.0.2s
./config --openssldir=/usr/local/ssl
make
sudo make install