<?php
$host = "w42v.add.ccc";
$return = array("TITLE"=>"monitor of mysql-proxy", "REV" => "OK");
$data   = array();

$failed  = false;
$warning = false;

function isProxyRunning($instance)	//检测进程是否存活
{
	$sub_data = array("METHOD" => "isProxyRunning");

	$cmd = "ps aux|grep '/usr/local/mysql-proxy/bin/mysql-proxy --defaults-file=/usr/local/mysql-proxy/conf/$instance.cnf'|grep -v grep";
	$ret = 1;
	exec($cmd, $res, $ret);

	if ($ret == 0)
	{
		$sub_data["REV"] = "OK";
	}
	else
	{
		$sub_data["REV"] = "FAILED";
		$GLOBALS["failed"] = true;
	}

	$GLOBALS["data"][$instance][] = $sub_data;
}
/*
function isPortAlive($instance, $proxy_port)	//检测端口是否存活
{
	$sub_data = array("METHOD" => "isPortAlive");

	$cmd = "netstat -an|grep $proxy_port";
	$ret = 1;
	exec($cmd, $res, $ret);

	if ($ret == 0)
	{
		$sub_data["REV"] = "OK";
	}
	else
	{
		$sub_data["REV"] = "FAILED";
		$GLOBALS["failed"] = true;
	}

	$GLOBALS["data"][$instance][] = $sub_data;
}
*/
function isPortAlive($instance, $proxy_port)	//检测端口是否存活
{
        $sub_data = array("METHOD" => "isPortAlive");

        $cmd = "mysql -h127.0.0.1 -P$proxy_port 2>&1";
        $handle = popen($cmd, "r");
        while (!feof($handle)) $contents .= fread($handle, 1024);
        if (strpos($contents, "2003") == FALSE)
        {   
                $sub_data["REV"] = "OK";
        }   
        else
        {   
                $sub_data["REV"] = "FAILED";
                $GLOBALS["failed"] = true;
        }   

        $GLOBALS["data"][$instance][] = $sub_data;
}

function getBackends($instance, $admin_port)		//检测连接数
{
	$sub_data = array("METHOD" => "getBackends");

	$cmd = "`which mysql` -h127.0.0.1 -P$admin_port -umysqlproxy -p25d9da7627501b6b -e 'SELECT * FROM BACKENDS'";
	$ret = 1;
	exec($cmd, $res, $ret);

	if ($ret != 0)
	{
		$sub_data["REV"] = "FAILED";
		$GLOBALS["failed"] = true;
	}	
	else
	{
		$backends = array();
		$key = array("backend_ndx", "address", "state", "type", "uuid", "connected_clients");
		$down_rw = $down_ro = 0;
		for ($i = 1; $i < count($res); ++$i)
		{
			$backend = array();
			$token = strtok($res[$i], "\t");
			$j = 0;

			while ($token != false)
			{
				$backend[$key[$j++]] = $token;
				$token = strtok("\t");
			}
			$backend[$key[$j++]] = $token;
			$backends[] = $backend;

			if ($backend["state"] == "down")
			{
				if ($backend["type"] == "rw") ++$down_rw;
				else ++$down_ro;
			}
		}

		if ($down_rw > 0)
		{
			$sub_data["REV"] = "FAILED";
			$GLOBALS["failed"] = true;
		}
		else if ($down_ro > 0)
		{
			$sub_data["REV"] = "WARNING: SOME SLAVES ARE DOWN";
			$GLOBALS["warning"] = true;
		}
		else
		{
			$sub_data["REV"] = "OK";
		}
		
		$sub_data["BACKENDS"] = $backends;
	}

	$GLOBALS["data"][$instance][] = $sub_data;
}

function getCpuMem($instance)		//检测CPU和内存占用
{
	$sub_data = array("METHOD" => "getCpuMem");
	$status = 0;

	$cmd = "ps aux|grep '/usr/local/mysql-proxy/bin/mysql-proxy --defaults-file=/usr/local/mysql-proxy/conf/$instance.cnf'|grep -v grep";
	$ret = 1;
	exec($cmd, $res, $ret);
	if ($ret != 0)
	{
		$sub_data["REV"] = "FAILED";
		$GLOBALS["failed"] = true;
	}
	else
	{
		list($user, $pid, $cpu, $mem, $vsz, $rss, $tty, $stat, $start, $time, $command) = split(" +", $res[0]);

		if ($cpu > 90)
		{
			$status += 1;
			$GLOBALS["warning"] = true;
		}
		$sub_data["CPU"] = $cpu;

		$maxVsz = 2 * 1024 * 1024;
		$maxRss = 512 * 1024;
		if ($vsz > $maxVsz || $rss > $maxRss)
		{
			$status += 2;
			$GLOBALS["warning"] = true;
		}
		$sub_data["VSZ"] = $vsz;
		$sub_data["RSS"] = $rss;
	}

	switch($status)
	{
	case 0:
			$sub_data["REV"] = "OK";
			break;
	case 1:
			$sub_data["REV"] = "WARNING: CPU IS BUSY";
			break;
	case 2:
			$sub_data["REV"] = "WARNING: MEMORY IS TOO LARGE";
			break;
	case 3:
			$sub_data["REV"] = "WARNING: CPU IS BUSY & MEMORY IS TOO LARGE";
			break;
	}

	$GLOBALS["data"][$instance][] = $sub_data;
}

function readLuaLog($instance)	//检测lua.log中有无新的错误记录
{
	$sub_data = array("METHOD" => "readLuaLog", "REV" => "OK");

	$cmd = "tail -n100 /usr/local/mysql-proxy/log/lua_$instance.log|egrep '\[FATAL\]|\[ERROR\]'";
	$ret = 1;
	exec($cmd, $res, $ret);

	if ($ret == 0 && count($res) > 0)	//如果日志中有FATAL或ERROR
	{
		for ($i = count($res)-1; $i >= 0; --$i)	//从后向前检查每条错误日志
		{
			$time = substr($res[$i], 1, 24);
			if (time() - strtotime($time) < 300)
			{
				$sub_data["REV"] = "WARNING: FATAL or ERROR in lua.log";
				$GLOBALS["warning"] = true;
				break;
			}
		}
	}

	$GLOBALS["data"][$instance][] = $sub_data;
}

function check()
{
	$cmd = "cat instances";
	$ret = 1;
	exec($cmd, $res, $ret);

	$start = 28; 
	for ($i = 0; $i < count($res); ++$i)
	{
		$line = ltrim(rtrim($res[$i]));

		$pos = strpos($line, '=');
		if ($pos == false) continue;

		$instance = rtrim(substr($line, 0, $pos));
		$ports = ltrim(substr($line, $pos+1));

		$pos = strpos($ports, ',');
		if ($pos == false) continue;

		$proxy_port = rtrim(substr($ports, 0, $pos));
		$admin_port = ltrim(substr($ports, $pos+1));

		isProxyRunning($instance);
		isPortAlive($instance, $proxy_port);
		getBackends($instance, $admin_port);
		getCpuMem($instance);
		readLuaLog($instance);
	}
}

check();

if ($warning)
{
	$return["REV"] = "WARNING";
}
if ($failed)
{
	$return["REV"] = "FAILED";
}
$return["DATA"] = $data;

echo serialize($return);
?>
