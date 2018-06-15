#!/usr/bin/python

__author__ = 'bingli'

import csv
import os
import sys
import time

from testlink import TestlinkAPIClient, TestLinkHelper
from testlink.testlinkerrors import TestLinkError

new_dir = 'executionLog'


def get_project():
    csv_data = csv.reader(open(csvFile, 'rU'))
    project = []
    for line_number, row in zip(xrange(1), csv_data):
        project.append(row[0])
        project.append(row[1])
        project.append(row[2])
    return project


def get_testcase():
    csv_data = csv.reader(open(csvFile, 'rU'))
    header = next(csv_data)
    testcase = []
    for index, row in enumerate(csv_data):
        if not row[2] == '':
            testcase.append((index+2, row[0], row[1], row[2]))  # return a list of tuples (index,execution,notes,testcase)
    return testcase[1:]


def run_report():
    testcases = get_testcase()
    project = get_project()

    run_num = 0
    failed_list = []
    test_project = project[0]
    test_plan = project[1]
    test_build = project[2]

    tls = TestLinkHelper().connect(TestlinkAPIClient)  # connect to Testlink
    testplan_id_result = tls.getTestPlanByName(test_project, test_plan)  # get test plan id
    testplan_id = testplan_id_result[0]['id']

    for i in testcases:
        index = i[0]
        test_result = i[1]
        test_notes = i[2]
        testcase_name = i[3]

        # testcase_id_result = tls.getTestCaseIDByName(testcase_name)  # get test case id
        # testcase_id = testcase_id_result[0]['id']
        # tls.reportTCResult(testcase_id, testplan_id, test_build, test_result, test_notes)

        try:
            testcase_id_result = tls.getTestCaseIDByName(testcase_name)  # get test case id
            testcase_id = testcase_id_result[0]['id']
            tls.reportTCResult(testcase_id, testplan_id, test_build, test_result, test_notes)
            run_num += 1
        except TestLinkError as e:
            failed_list.append((index, testcase_name, e))

    if len(failed_list) > 0:
        log_name = 'logFile_%s.txt' % time.time()
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        dest_dir = os.path.join(curr_dir, new_dir)
        try:
            os.makedirs(dest_dir)
        except OSError:
            pass
        log_file = os.path.join(dest_dir, log_name)
        with open(log_file, 'w') as f:
            for item in failed_list:
                f.write('-----'.join(str(i) for i in item) + '\n')

    print '%s test case(s) has been written into log file.' % len(failed_list)
    print '%s test case(s) has been executed.' % run_num

# csvFile = 'OTC-310.csv'
# run_report()

if __name__ == '__main__':
    csvFile = sys.argv[1]  # 'OTC-8.csv'
    run_report()
