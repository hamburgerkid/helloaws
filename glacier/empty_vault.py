#!/usr/bin/python
# coding: UTF-8
import sys
sys.path.append('/usr/local/lib/python2.7/site-packages/')
import boto, datetime, getopt, json, time, traceback
from boto.glacier.layer1 import Layer1
from boto.regioninfo import RegionInfo
from boto.sns import SNSConnection
from boto.sqs.connection import SQSConnection
from boto.sqs.queue import Queue

### Function.
def usage():
	print 'empty_valt.py -v <vault_name>';
	print 'Enjoy!';
	sys.exit();

def log(message):
	d = datetime.datetime.today();
	print '%s-%s-%s %s:%s:%s.%s %s' % (d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond, message);

### Main.
vault_name = '';
try:
	opts, args = getopt.getopt(sys.argv[1:], 'hv:');
except getopt.GetoptError:
	usage();
for opt, arg in opts:
	if opt == '-h':
		usage();
	elif opt in ('-v'):
		vault_name = arg;
if vault_name == '':
	usage();

log('Start!');
log('Configure Property.');
aws_account_id = boto.config.get('Credentials', 'aws_account_id');
aws_access_key_id = boto.config.get('Credentials', 'aws_access_key_id');
aws_secret_access_key = boto.config.get('Credentials', 'aws_secret_access_key');
aws_region_name = boto.config.get('Environments', 'aws_region_name');
job_output_file = boto.config.get('Environments', 'job_output_file');

log('Set up SNS and SQS.');
sns_region = RegionInfo(name = aws_region_name, endpoint = 'sns.' + aws_region_name + '.amazonaws.com');
sns_con = SNSConnection(aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region = sns_region);
sns_topic = sns_con.create_topic('empty_vault-topic');

# SQS endpoint URL (sqs.xxx.amazonaws.com) doesn't work in boto 2.8.0 due to certification error. 
# sqs_region = RegionInfo(name = aws_region_name, endpoint = 'sqs.' + aws_region_name + '.amazonaws.com')
# sqs_con = SQSConnection(aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region = sqs_region);
sqs_con = SQSConnection(aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key);
sqs_queue = sqs_con.create_queue('empty_vault-queue');

sns_topic_arn = sns_topic['CreateTopicResponse']['CreateTopicResult']['TopicArn'];
sns_con.subscribe_sqs_queue(sns_topic_arn, sqs_queue);

log('Point to Glacier vault.');
glacier_layer1 = Layer1(aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region_name = aws_region_name);
glacier_layer1.describe_vault(vault_name);

log('Kick Glacier job.');
job_data = dict(Description = 'Retrieve archive info to empty vault.', Format = 'JSON', SNSTopic = sns_topic_arn, Type = 'inventory-retrieval');
job_info = glacier_layer1.initiate_job(vault_name, job_data);
job_id = job_info['JobId'];
glacier_layer1.describe_job(vault_name, job_id); 

log('Wait for Glacier job completion.');
confirmed = False;
while confirmed == False:
	log('Sleep for a while...');
	time.sleep(3600.0); # 1 hour.
	log('Try to receive SQS message.');
	sqs_msg_list = sqs_con.receive_message(sqs_queue, wait_time_seconds = 20);
	if sqs_msg_list:
		log('SQS message received and Check Glacier job status.');
		job_status_list = glacier_layer1.list_jobs(vault_name, completed = True, status_code = 'Succeeded');
		for job_status in job_status_list['JobList']:
			if job_status['JobId'] == job_id:
				log('Glacier job completion confirmed.');
				confirmed = True;
				break;

log('Get Glacier job output.');
job_output = glacier_layer1.get_job_output(vault_name, job_id);
job_output_file = open(job_output_file, 'w');
job_output_file.write(json.dumps(job_output));
job_output_file.close();

log('Delete all archives in Glacier vault.');
for idx, archive_info in enumerate(job_output['ArchiveList']):
	if idx % 100 == 0:
		log('Renew Glacier object to avoid socket error while archive deletion.');
		glacier_layer1 = Layer1(aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region_name = aws_region_name);
	try:
		glacier_layer1.delete_archive(vault_name, archive_info['ArchiveId']);
	except:
		fail_msg = 'Fail to delete archive (%s) but Proceed to next.' % archive_info['ArchiveId'];
		log(fail_msg);
		log(traceback.format_exc());
		continue;

log('Delete SQS queue to clean up.');
sqs_con.delete_queue(sqs_queue);

log('Finish!');
