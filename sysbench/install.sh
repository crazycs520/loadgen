tar -xvf 1.0.20.tar.gz
cd sysbench-1.0.20/
./autogen.sh
./configure --prefix=/usr/sysbench/ --with-mysql-includes=/usr/include/mysql/ --with-mysql-libs=/usr/lib64/mysql/ --with-mysql
make
sudo make install
sudo cp -r /usr/sysbench/share/sysbench/* /usr/sysbench/bin/
sudo rm /usr/local/bin/sysbench
sudo ln -s /usr/sysbench/bin/sysbench /usr/local/bin/sysbench
