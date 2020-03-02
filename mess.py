#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
import json
# import json
import xmltodict
import xml.etree.ElementTree as ET

def get_value(node, kv):
    if isinstance(node, list):
        for i in node:
            for x in get_value(i, kv):
               yield x
    elif isinstance(node, dict):
        if kv in node:
            yield node[kv]
        for j in node.values():
            for x in get_value(j, kv):
                yield x

def get_patient_data(patient):
    fields = ['FULL_NAME', 'PATIENT_ID', 'BIRTH_DATE']
    patient_data = { k:v for (k,v) in patient.items() if k in fields }

    return patient_data

def get_study_data(study):

    fields = [
        'VISIT_DATE', 'SITE', 'DISPLAY_NAME', 'EXAM_TIME', 'SPHERE',
        'CYLINDER', 'AXIS', 'PUPIL_DIAMETER', 'EXAM_DURATION',
        'FALSE_NEGATIVE_PERCENT', 'FALSE_POSITIVE_PERCENT', 'TRIALS', 'ERRORS',
        'FOVEAL_THRESHOLD'
    ]

    study_data = {}

    for f in fields:
        if f == 'SPHERE':
            study_data['TRIAL_RX_SPHERE'] = ''
            study_data['DISTANCE_RX_SPHERE'] = ''
            # there are 2x spheres
            spheres = list(get_value(study,f))
            if len(spheres) > 0:
                study_data['TRIAL_RX_SPHERE'] = spheres[0]
                study_data['DISTANCE_RX_SPHERE'] = spheres[1]
        else:
            try:
                study_data[f] = list(get_value(study,f))[0]
            except IndexError:
                study_data[f] = ''

    return study_data

def get_threshold_data(threshold_test):
    threshold_data = {}

    tplots = threshold_test['THRESHOLD_SITE_LIST']['THRESHOLD_XY_LOCATION']
    i = 1
    for t in tplots:
        k = '_'.join(['TH', str(i)])
        v = t['THRESHOLD_1']
        threshold_data[k] = v
        i += 1

    # get Stats fields between test plot data points
    stats_fields = [
        'LOW_PATIENT_RELIABILITY_STATUS', 'MD', 'PSD', 'VFI'
    ]
    for f in stats_fields:
        try:
            statpac = get_value(threshold_test['STATPAC'],f)
            v = list(statpac)[0]
        except IndexError:
            # print('Failed to find a valid STATPAC!')
            return None
        except KeyError:
            # Some cases without a 'STATPAC' node can be ignored.
            # print('Failed to find a valid STATPAC!')
            return None
        threshold_data[f] = v

    tdplots = threshold_test['STATPAC']['TOTAL_DEVIATION_VALUE_LIST']['TOTAL_DEV_XY_LOCATION']
    i = 1
    for td in tdplots:
        k = '_'.join(['TD', str(i)])
        v = td['TOTAL_DEVIATION_VALUE']
        threshold_data[k] = v
        i += 1

    pdplots = threshold_test['STATPAC']['PATTERN_DEVIATION_VALUE_LIST']['PATTERN_DEV_XY_LOCATION']
    i = 1
    for pd in pdplots:
        k = '_'.join(['PD', str(i)])
        v = pd['PATTERN_DEVIATION_VALUE']
        threshold_data[k] = v
        i += 1

    return threshold_data


def output_data(study_results, out_file=sys.stdout):

    print('\nFound %s valid test results to output.' % len(study_results) )

    if out_file == sys.stdout:
        print('Outputting results to stdout.\n\n')
        writer = csv.DictWriter(out_file, fieldnames=study_results[0].keys())
    else:
        print('Outputting results to %s \n\n' % str(out_file) )
        of = open(out_file, 'w')
        writer = csv.DictWriter(of, fieldnames=study_results[0].keys())


    output_errors = []
    writer.writeheader()
    for row in study_results:
        try:
            writer.writerow(row)
        except ValueError:
            output_errors.append(row)

    if output_errors:
        return False, output_errors
    else:
        return True, output_errors

def output_errors(bad_records, error_file):

    print('\nFound %s bad records during processing.' % len(bad_records) )

    print('Outputting bad results to %s \n\n' % str(error_file) )

    with open(error_file, 'w') as o:
        for b in bad_records:
            o.write(str(b))

def main(in_file, out_file, error_file):
    # get data
    patients = []
    with open(in_file, 'r') as f:
        print('Parsing file: %s' % in_file)
        print('File size: %s bytes' % str(os.path.getsize(in_file)))
        xml_dict = xmltodict.parse(f.read())
        # print('Found XML data with %s entries' % len(xml_dict.items()))

        patients = xml_dict['HFA_EXPORT']['PATIENT']
        print('Found %s patient records' % len(patients))

    ## alternate approach
    # tree = ET.parse(file)
    # xml_data = tree.getroot()
    # xml_str = ET.tostring(xml_data, encoding='utf-8', method='xml')
    # xml_dict = dict(xmltodict.parse(xml_str))

    # get records
    study_results = []
    bad_records = []
    for patient in patients:
        patient_data = get_patient_data(patient)
        study_data = get_study_data(patient['STUDY'])
        threshold_data = {}
        try:
            threshold_test = list(get_value(patient,'THRESHOLD_TEST'))[0]
            threshold_data = get_threshold_data(threshold_test)
        except IndexError:
            bad_records.append(json.dumps(patient_data))

        if threshold_data:
            study_results.append({
                **patient_data,
                **study_data,
                **threshold_data
                })
        else:
            bad_records.append(json.dumps(patient_data))


    results, errors = output_data(study_results, out_file)

    bad_records.extend(errors)
    if bad_records:
        output_errors(bad_records, error_file)


if __name__ == '__main__':
    in_file = sys.argv[1]
    out_file = in_file.split('.')[0] + '_data.csv'
    error_file = in_file.split('.')[0] + '_errors.txt'
    main(in_file, out_file, error_file)
