#!/usr/bin/perl
use DBI;
use lib qw(/etc/nwzdb /usr/local/lib/perl);
use DBConf;
use Mail::Mailer;
use POSIX;

($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime;
$now = sprintf("%04d%02d%02d%02d%02d%02d", $year + 1900, $mon + 1, $mday, $hour, $min, $sec);
$now_time = sprintf("%1d", $hour);

$dir = $rrddir;

				#�ð��� 250�� ���� ������ ������ �Ʒ��� �����Ų��.
while ( ($tns,$value) =  each %dbconf)
{
 next if $tns eq 'LONDON';
 next if $tns eq 'CHICAGO';
 next if $tns eq 'CAIRO';
 next if $tns eq 'LIMA';
 next if $tns eq 'HONGKONG';
 next if $tns eq 'SYDNEY';
 next if $tns =~ /^BACKUP_/;

 $username = $value->{'username'};
 $password = $value->{'password'};

 my $dbh;
 if($dbh = DBI->connect("dbi:Oracle:$tns",$username,$password,{AutoCommit=>0, ora_envhp=>0}) )
 {
	#print "Connected to $tns\n";
 }
 else
 {
	print "Connection attempt to $tns is failed , Skip\n";
	next;
 }

$tsql = <<EOF;
select tbs,round(pctused,1) pct from
(
SELECT  fi.TABLESPACE_NAME              tbs,
                ( sum(fi.BYTES) - NVL(free,0) ) *100 /
                      sum(fi.BYTES) pctused
          FROM  sys.dba_data_files fi,dba_tablespaces tbs,
               (SELECT TABLESPACE_NAME,
                       sum(BYTES) free
                  FROM sys.dba_free_space
                 GROUP BY TABLESPACE_NAME) ex
         WHERE fi.tablespace_name = ex.tablespace_name(+)  AND fi.tablespace_name = tbs.tablespace_name
         GROUP BY fi.tablespace_name,free
)
where pctused >95 and tbs not like 'SYS%' and tbs not like 'UNDO%'
order by pctused desc
EOF


$esql=<<EOA;
SELECT username, 
to_char(timeslot,'hh24:"00~"HH24:"59"')  timeslot,
               sum (CASE
                WHEN error_code NOT BETWEEN 20000 AND 20999
                THEN cnt_in_timeslot
                ELSE 0
        END) sys_cnt 
FROM    error_log
WHERE   timeslot      > sysdate -1/24
    AND error_code not in (1,1722)
    AND program NOT  IN ('SQL Developer' , 'OrangeMain.exe', 'SQLNav4.exe','SQLTools.exe')
    AND username NOT IN ('SYS','SYSTEM','SCOTT','PERFSTAT','OUTLN','DBSNMP','ORDSYS','MDSYS','WMSYS','ORABM','DBCON','MADMIN','DBTOOL','CALCS','IPOP','BUGSMDRM')
    AND username NOT IN
        (SELECT username FROM dba_users WHERE account_status <> 'OPEN'
        )
    AND username NOT IN
        ( SELECT grantee FROM dba_role_privs WHERE granted_role='NWIUSER'
        )
group by username,to_char(timeslot,'hh24:"00~"HH24:"59"')
EOA

$qsql=<<EOB;
select service,count(*) cnt
from ql_err
where client_side='Y' and regdate > sysdate -1/24
and service not like '%_backup'
group by service order by cnt desc
EOB

# ���̺����̽� �迭�γֱ�.
	my $s = $dbh->prepare($tsql);
	$s->execute();
	while (($tbs, $pct) = $s->fetchrow_array())
	{
	#	���� ������ �迭�� �ִ´�.
		push @tbs_array,(["$tns","$tbs","$pct%"]);
	 }

# error �迭�� �ֱ� =animato�� error�� �������� �����Ƿ� ���� 
unless ($tns eq 'ANIMATO'){
	my $s = $dbh->prepare($esql);
	$s->execute();
	while (($schema,$timeslot,$syscnt) = $s->fetchrow_array())
	{
	# 0�ú��� 8�ñ����� 200�� �� ���Ĵ� error ������ 100�� �̻��̸�  ������ �迭�� �ִ´�
		if($now_time>=0 && $now_time<=8){

			if($syscnt>300){
			    push @err_array,(["$tns","$schema","$timeslot","$syscnt"]);
			}else{
				next;
			}
		}else {
            if($syscnt>100){
                push @err_array,(["$tns","$schema","$timeslot","$syscnt"]);
            }else{
                next;
            }
		}
	}

	if($tns eq 'DENVER'){
	    my $s = $dbh->prepare($qsql);
    	$s->execute();
	    while (($service,$syscnt) = $s->fetchrow_array())
    	{
	    # 0�ú��� 8�ñ����� 200�� �� ���Ĵ� error ������ 100�� �̻��̸�  ������ �迭�� �ִ´�
	        if($now_time>=0 && $now_time<=8){

    	        if($syscnt>300){
        	        push @client_array,(["$service","$syscnt"]);
            	}else{
                	next;
	            }
    	    }else {
        	    if($syscnt>100){
            	    push @client_array,(["$service","$syscnt"]);
	            }else{
    	            next;
        	    }
	        }
	    }
	}
}
 $s->finish();
 $dbh->disconnect;
 
}

# denver������ �����´�.
while ( ($tns,$value) =  each %dbconf)
{
	next unless $tns eq 'DENVER';

 	$username = $value->{'username'};
	$password = $value->{'password'};
	
 if($dbh = DBI->connect("dbi:Oracle:$tns",$username,$password,{AutoCommit=>0}) )
 {
    #print "Connected to $tns\n";
 }
 else
 {
    print "Connection attempt to $tns is failed , Skip\n";
    next;
 }
}

sms_submit('tbs_array',1,'TABLESPACE');
sms_submit('err_array',2,'SqlError','<TABLE border=1> <TR align=right><TH>INSTANCE </TH><TH align=left>��Ű��</TH><TH>TIMESLOT</TH><TH>System Exception</TH></TR>');
sms_submit('client_array',3,'Clientside SQL Error','<TABLE border=1> <TR align=right><TH>sqlrelay����</TH><TH align=left>Ƚ��/1�ð�</TH></TR>');

sub sms_submit{
	my @sms_array; #�Լ�ȣ��ø��� �ʱ�ȭ 
	my @sms_array_100;
	my @body_char;
	my @onli_body_char;

	my @phoneNo;

	my $val;
	my $body;
	my $body2;
	$val=$_[1];
	$body=$_[3];
	$body2=$_[3];
	$array_nm=$_[0];
	$sms_mail;
	$only_mail;

#���̺����̽��� �뷮�� 92%�ʰ��Ȱ��� ���� 3�ð����� sms�߼��� �� ���̺����̽����� �ִ��� Ȯ���� ���ٸ� sms�߼� �迭�� �ִ´�.
	for $i (0 ..$#$array_nm){ # �� $tbs_array�� �迭 ��ü ũ�⸸ŭ ���°Ͱ� ����. 
		if($array_nm eq 'tbs_array'){
			$log=<<EOZ;
				select count(1) as cnt from smslog_tbs where tns_nm='$$array_nm[$i][0]' and tbs_nm='$tbs_array[$i][1]'  and crt_dt>sysdate-3/24
EOZ
			my $selcnt=$dbh->prepare($log);
			$selcnt->execute();
                while (($cnt) = $selcnt->fetchrow_array())
                {
                    if($cnt==0){
                    #   3�ð��̳��� �������� ���� �����͸� mail�߼� �迭�� �ִ´�.
						push @sms_array,"$$array_nm[$i][0]-$$array_nm[$i][1]-$$array_nm[$i][2]/";
                    }else{
                        next;
                    }
                }
		}elsif($array_nm eq 'client_array'){
            $log=<<EOZ;
                select count(1) as cnt from SMSLOG_CLIENTERR where service='$$array_nm[$i][0]' and crt_dt>sysdate-3/24
EOZ
                my $selcnt=$dbh->prepare($log);
                $selcnt->execute();

                while (($cnt) = $selcnt->fetchrow_array())
                {
                    if($cnt==0){
                            push @sms_array,"$$array_nm[$i][0]-1�ð��̳�����:$$array_nm[$i][1]/";
                            push @body_char ,"<TR><TD> <A href=http://idb.neowiz.com/tool/qlog_error_list.php?service=$$array_nm[$i][0]>$$array_nm[$i][0]</A></TD><TD>$$array_nm[$i][1]</TD></TR>";
                    }else{
                        next;
                    }
                }
		}else{
            $log=<<EOZ;
                select count(1) as cnt from smslog_sqlerr where tns_nm='$$array_nm[$i][0]' and crt_dt>sysdate-3/24
EOZ
			$mlog=<<EOZ;
                select count(1) as cnt from maillog_sqlerr where tns_nm='$$array_nm[$i][0]' and crt_dt>sysdate-3/24
EOZ
			if($$array_nm[$i][3]>250){
				my $selcnt=$dbh->prepare($log);
				$selcnt->execute();

				#�ð��� 250�� �̻� ������ �����°� �Ʒ� ������ �����Ų��. 
		        while (($cnt) = $selcnt->fetchrow_array())
		        {
        		    if($cnt==0){
                		    push @sms_array,"$$array_nm[$i][0]-schema:$$array_nm[$i][1]-timeslot:$$array_nm[$i][2]-system:$$array_nm[$i][3]/";
							push @body_char ,"<TR><TD>$$array_nm[$i][0]</TD><TD> <a href=http://idbtool.neowiz.com/tool/runtool.php?tool=chk_error_log&tnsname=$$array_nm[$i][0]&owner=$$array_nm[$i][1]> $$array_nm[$i][1]</a> </TD><TD align=right>$$array_nm[$i][2]</TD><TD align=right>$$array_nm[$i][3]</TD></TR>";
            		}else{
                		next;
            		}
		        }

			}else{
				my $mselcnt=$dbh->prepare($mlog);
				$mselcnt->execute();
				#�ð��� 250�� ���� ������ ������ �Ʒ��� �����Ų��.
         		while (($mcnt) = $mselcnt->fetchrow_array())
		        {
		            if($mcnt==0){
		            #   3�ð��̳��� �������� ���� �����͸� mail�߼� �迭�� �ִ´�.
							push @sms_array_100,"$$array_nm[$i][0]-schema:$$array_nm[$i][1]-timeslot:$$array_nm[$i][2]-system:$$array_nm[$i][3]/";
		                    push @onli_body_char ,"<TR><TD>$$array_nm[$i][0]</TD><TD><a href=http://idbtool.neowiz.com/tool/runtool.php?tool=chk_error_log&tnsname=$$array_nm[$i][0]&owner=$$array_nm[$i][1]> $$array_nm[$i][1] </a> </TD><TD align=right>$$array_nm[$i][2]</TD><TD align=right>$$array_nm[$i][3]</TD></TR>";
		            }else{
		                next;
        			}
				}
			}
        }
    }

    $t= join("\n",@sms_array);
	$g= join("\n",@sms_array_100);
    $n=length($t);
	$m=length($g);
    $sms_mail.= join("\n","@body_char");
	$sms_mail_n=length($sms_mail);
	#100���ϴ� body�� ������ƾ� ������ ���� �´�.
	$only_mail.=join("\n","@onli_body_char");
	$only_mail_n=length($only_mail);

# sms�� �߼��ؾ��� ����� �����ϸ� �Ʒ� if������ ����.
# 2011.4.26 �߰� - ���̺����̽��� ���� ������ �ʴ´�.
    if($n || $m){

		#���Ϸθ� �������ϴ� ���� �� if���� ź��.
     if($only_mail_n){
		$body.=$only_mail;
        #���� �߼� ����~~~~~~~~~~~~~~~~~~~~~~~~
        $mail = Mail::Mailer->new("sendmail");
        $subj = '[SQL����]�ð��� 100�� �̻��� �����߻�';
        $wday = ((localtime)[6]);
        $hour = ((localtime)[2]);

		$body.="</TABLE>";
        for $to ($teamDL)
        {
            $mail->open({From => 'colasarang@neowiz.com',
                     To => $to,
                     'Content-type'=> 'text/html; charset=euc-kr',
                     Subject => $subj
            }) or die "Can't open: $!\n";

        print $mail $css.$body;
        $mail->close();
        }

        for $i ( 0 ..$#sms_array_100)
        {
            ($log_tns,$log_tbs)=split('-',$sms_array_100[$i]);
                $sms_log= <<EOT;
                            merge INTO maillog_sqlerr t1 USING
                                (SELECT '$log_tns' AS tns_nm FROM dual) t2
                            ON (t1.tns_nm=t2.tns_nm )
                            WHEN matched THEN
                                UPDATE SET t1.crt_dt=sysdate
                            WHEN NOT matched THEN
                                INSERT  (id,tns_nm,crt_dt)
                                VALUES  (smslog_seq.nextval, t2.tns_nm,sysdate)
EOT
            $ex2=$dbh->prepare($sms_log);
            $ex2->execute();
        }
	}

	#���ϰ� sms�Ѵ� �������ϴ� ��쳪 ���̺����̽� ���� �� if���� ź��.
	if($sms_mail_n || ($array_nm eq 'tbs_array')){
		$body2.=$sms_mail;
        #���� �߼� ����~~~~~~~~~~~~~~~~~~~~~~~~
        $mail = Mail::Mailer->new("sendmail");

        unless ($array_nm eq 'tbs_array'){
			if($array_nm eq 'err_array'){
            	$subj = '[SQL����]�ð��� 250�� �̻��� �����߻�';
			}else{
				$subj = '[Clientside SQL����]�ð��� 100�� �̻��� �����߻�';
			}
	        $wday = ((localtime)[6]);
	        $hour = ((localtime)[2]);

			$body2.="</TABLE>";	
    	    for $to ($teamDL)
        	{
            	$mail->open({From => 'colasarang@neowiz.com',
                	     To => $to,
                    	 'Content-type'=> 'text/html; charset=euc-kr',
                     	Subject => $subj
	            }) or die "Can't open: $!\n";
	        print $mail $css.$body2;
    	    $mail->close();
        	}
		}

        #���Ϲ߼� �� ~~~~~~~~~~~~~~~~~~~~~~~~~

        if($array_nm eq 'tbs_array'){
            @phoneNo=qw(01032004422 01087292910 01091950982 01020240406 01091925324 01089033427 01091055552);
        }else{
            @phoneNo=qw(01032004422 01087292910 01091950982 01020240406 01091925324 01043836675 01054731694 01089033427 01091055552);
        }

        #mms�� ��Ż��ʿ��� ������ �����Ƿ� ������  sms�� 80����Ʈ�� �߶� �������Ѵ�. �Ǿտ� ������ �ٿ����ϹǷ� 66byte�� �ڸ���.
        if($n>68){
            $start=0;
            $end=68;
            $sms_cnt=ceil($n/66);
            for($i=1; $i<=$sms_cnt; $i++){
                $ms=substr $t,$start,$end;
                $fir=$_[2];
                $msg="[$fir-$i]$ms";

                $sms = <<EOQ;
                 INSERT INTO bugslog.uds_msg\@bali(MSG_TYPE, CMID, REQUEST_TIME, DEST_PHONE, SEND_PHONE, MSG_BODY)
                VALUES(0,?,SYSDATE,?, '0220330089', '$msg')
EOQ
                $start+=68;
                $fir='';
                my $loop=1;
                    # �ڵ�����ȣ �迭�� while�� ������!
                foreach $phone(@phoneNo){
                        #cmid�� �������̾�� �ϹǷ� bind������ ��ġ���ʴ� ���� �ֵ��� �Ѵ�.
                    $bind="$now$val$loop$i";
                    my $ex=$dbh->prepare($sms);
                    $ex->execute($bind,$phone);
                    $loop+=1;
                }
            }
        }else{
        #68byte ������ ���� �Ʒ�ó�� ��������
            $fir=$_[2];
            $msg="[$fir]$t";
            $sms = <<EOQ;
                 INSERT INTO bugslog.uds_msg\@bali(MSG_TYPE, CMID, REQUEST_TIME, DEST_PHONE, SEND_PHONE, MSG_BODY)
                VALUES(0,?,SYSDATE,?, '0220330089', '$msg')
EOQ
            my $loop=1;
                    # �ڵ�����ȣ �迭�� while�� ������!
            foreach $phone(@phoneNo){
                        #cmid�� �������̾�� �ϹǷ� bind������ ��ġ���ʴ� ���� �ֵ��� �Ѵ�.
                $bind="$now$val$loop$i";
                my $ex=$dbh->prepare($sms);
                $ex->execute($bind,$phone);
                $loop+=1;
            }
        }

        #3�ð��̳��� �������� ���� �����ʹ� �ٽ� smslog�� �����.(�� ���� sms�߼������� �����)
        for $i ( 0 ..$#sms_array)
        {
            ($log_tns,$log_tbs)=split('-',$sms_array[$i]);
            if($array_nm eq 'tbs_array'){
                $sms_log= <<EOT;
                            merge INTO smslog_tbs t1 USING
                                (SELECT '$log_tns' AS tns_nm, '$log_tbs' AS tbs_nm FROM dual) t2
                            ON (t1.tns_nm=t2.tns_nm AND t1.tbs_nm=t2.tbs_nm)
                            WHEN matched THEN
                                UPDATE SET t1.crt_dt=sysdate
                            WHEN NOT matched THEN
                                INSERT  (id,tns_nm,tbs_nm,crt_dt)
                                VALUES  (smslog_seq.nextval, t2.tns_nm,t2.tbs_nm,sysdate)
EOT
            $ex2=$dbh->prepare($sms_log);
            $ex2->execute();

            }elsif($array_nm eq 'err_array') {
                $sms_log= <<EOT;
                            merge INTO smslog_sqlerr t1 USING
                                (SELECT '$log_tns' AS tns_nm FROM dual) t2
                            ON (t1.tns_nm=t2.tns_nm )
                            WHEN matched THEN
                                UPDATE SET t1.crt_dt=sysdate
                            WHEN NOT matched THEN
                                INSERT  (id,tns_nm,crt_dt)
                                VALUES  (smslog_seq.nextval, t2.tns_nm,sysdate)
EOT

				$mail_log=<<EOT;
                            merge INTO maillog_sqlerr t1 USING
                                (SELECT '$log_tns' AS tns_nm FROM dual) t2
                            ON (t1.tns_nm=t2.tns_nm )
                            WHEN matched THEN
                                UPDATE SET t1.crt_dt=sysdate
                            WHEN NOT matched THEN
                                INSERT  (id,tns_nm,crt_dt)
                                VALUES  (smslog_seq.nextval, t2.tns_nm,sysdate)
EOT
            $ex2=$dbh->prepare($sms_log);
            $ex2->execute();
            $ex2=$dbh->prepare($mail_log);
            $ex2->execute();
            }else{
				$client_log=<<EOT;
                            merge INTO SMSLOG_CLIENTERR t1 USING
                                (SELECT '$log_tns' AS service FROM dual) t2
                            ON (t1.service=t2.service )
                            WHEN matched THEN
                                UPDATE SET t1.crt_dt=sysdate
                            WHEN NOT matched THEN
                                INSERT  (id,service,crt_dt)
                                VALUES  (smslog_seq.nextval, t2.service,sysdate)
EOT
            $ex2=$dbh->prepare($client_log);
            $ex2->execute();
			}
        }
	}
  }

}
 $dbh->disconnect;
