#!/usr/bin/env python
"""
dcm_convert.py
By: Michael Durnhofer (mdurn@ucla.edu)
11/5/2009

Searches for all DICOM files in a selected path and allows converion to NIfTI,
Compressed NIfTI, or Analyze formats. See options for help.


========= Change Log (mm.dd.yyyy)     =========
========= Format: mm.dd.yyyy - Author =========

04.06.2011 - Michael Durnhofer
  - Added '-l' option for specifying a separate log file directory
  - Added '-m' (metadata) option for saving dicom headers (using this
    option requires AFNI's dicom_hdr script)
  - Added -s (sync) option to disable converting dicom files if the 
    destination files already exist
  - New functions: check_dicom_hdr, write_header

10.20.2010 - James A. Kyle
  - Changed the match pattern for building the dicom file list so that it
    should match any numeric based DICOM file naming convention. This was
    done to accommodate files from the new NPI scanner
  - Changed method names to conform to PEP style recommendations
  - Changed Popen calls to wait till completion to avoid race conditions
  - Changed Popen calls to shutil or os calls where appropriate
  - Changed Popen shell=True calls into PIPES (for safety/security)
  - Changed email so that it is only sent after all jobs are completed instead
    of after the last submitted job is completed since there is no guarantee 
    of order or that the last job is completed last.
  - Created a default "batch" mode that assumes you wish to convert all 
    dicoms found in the current directory.
  - Now all logs for are placed in a "logs" directory in the same location
    as the 'converted' directory
"""
import os
import re
import sys

from   subprocess      import Popen
from   subprocess      import PIPE
from   optparse        import OptionParser
from   email.mime.text import MIMEText

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    def disable(self):
	self.HEADER = ''
	self.OKBLUE = ''
	self.OKGREEN = ''
	self.WARNING = ''
	self.FAIL = ''
	self.ENDC = ''

class Error(Exception):
    """Base class for exceptions in this module."""

    def __init__(self, message):
        super(Error, self).__init__()
        self.message = message

    def __repr__(self):
        return "Error(message={0})".format(self.message)


class InvalidOptionError(Error):
    def __init__(self, message):
        super(InvalidOptionError, self).__init__(message)

    def __repr__(self):
        return "InvalidOptionError(message={0})".format(self.message)


class FileTypeError(Error):
    """Exeption raised for errors choosing an output file type.

    Attributes:
	message -- explanation of the error
    """

    def __init__(self, message):
	self.message = message
    def __str__(self):
	return self.message

def get_dicom_list(rootDir):
    dcm_list = []
    for path, dirs, files in os.walk(rootDir):
        for f in files:
            fpath = os.path.join(path, f)
            # some dicom files have an extension
            # all the ones I've seen have numeric roots though
            froot = os.path.splitext(f)[0]
            if froot.isdigit() and int(froot) == 1:
                    dcm_list.append(fpath)
    return dcm_list

def select_root():
    msg = ("{0}Select root directory for "
          "DICOM file search (e.g. /home/user/foo):{1} ")

    return raw_input(msg.format(bcolors.HEADER, bcolors.ENDC))

def select_filetype():
    """
    Allows the user to specify the output file type of the image as nifti,
    compressed nifti, or analyze.
    """

    prompt = """Select the output file type:
[1] NIfTI (.nii)
[2] Compressed NIfTI (.nii.gz)
[3] Analyze (.img)
: """

    errmsg = """{0}Invalid input. See below for valid input:{1}

1. For uncompressed nifti files, e.g. .nii

2. For compressed nifti, e.g. nii.gz

3. For analyze, e.g. an img .hdr pair""".format(bcolors.FAIL, bcolors.ENDC)

    outputs   = ["nii", "nii.gz", "img"]
    selection = raw_input(prompt)
    index     = None
    ext       = None

    try:
        index = int(selection)
    except ValueError, e:
        msg = "{0} is not a valid option!"
        raise InvalidOptionError(msg.format(selection))

    if index is not None:
        ext = outputs[index]

    return ext

def parse_options():
    HELP = """Convert a DICOM image series to a specified image format using a
user-specified conversion program (the default is mri_convert).
Currently only mri_convert is supported.

All DICOM images found in the user-selected root directory (and all
subdirectories) are converted to the selected format (which are stored by
default in the folder \'converted\' located in the parent directory of the
converted image). By default, the name of the converted image is the same as
the original image but in all lower-case letters."""
    
    e_help = """Enables emailing of job completion notification. Takes 1 
argument (the recipient user ID on hoffman)."""

    parser = OptionParser(usage="%prog [options]", version = "%prog 1.2", 
                          description = HELP)

    parser.add_option('-t', '--tool', action='store', type='string',
                      dest='tool', default='mri_convert',
                      help='image conversion program to convert DICOM image')

    parser.add_option('-f', '--filetype', action='store', type='string',
                      dest='filetype', default="nii.gz",
                      help='filetype to convert to (nii, nii.gz, img)')

    parser.add_option('-d', '--destination', action='store', type='string',
                      dest='destination', default=None,
                      help='output directory of the converted image')
    
    parser.add_option('-r', '--root', action='store', type='string',
                      dest='root', default="./",
                      help='root directory to search for DICOM files')

    parser.add_option('-e', '--email', action='store_true',
                      dest='email', default=False, help=e_help)

    parser.add_option("-b", "--batch", action="store_true", dest="batch", 
                      default=False, 
                      help="Convert all files in current directory.")

    parser.add_option("-l", "--logdir", action='store', type='string',
    	              dest="logdir", default=None,
		      help="output directory of log files")

    parser.add_option("-m", "--meta", action="store_true", dest="header",
                      default="False",
		      help="save dicom header (requires AFNI dicom_hdr)")

    parser.add_option("-s", "--sync", action="store_true", dest="sync",
                      default="False",
		      help="Do not write overexisting files.")
    
    options, args = parser.parse_args()
    
    return options 

def create_qsub(command, logdir=""):
    qsub = """#!/bin/bash
#$ -V
#$ -cwd
#$ -j y
#$ -N dcm_convert
#$ -o {logpath}.o
#$ -l h_data=1024M,h_rt=24:00:00
{command}
    """.format(command=command, logpath=os.path.join(logdir, "dcm_convert"))
    return qsub

def check_dicom_hdr():
    p = Popen(['which', 'dicom_hdr'], stdout=PIPE, stderr=PIPE)
    [stdout, stderr] = p.communicate()
    if not stderr == '':
        msg = """-m cannot be used, 'which dicom_header' returns:
{stderr}

Please add AFNI dicom_hdr to your PATH."""

        try:
	    raise InvalidOptionError(msg.format(stderr=stderr))
	except InvalidOptionError, e:
	    print e.message
	    exit(1)

def write_header(dcm_file, header_filepath):
    cmd = ["dicom_hdr", dcm_file]
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)
    [stdout, stderr] = p.communicate()
    if not stderr == '':
        header_err_filepath = '.'.join([header_filepath, "err"])
        header_err_file = open(header_err_filepath, 'w')
        header_err_file.write(stderr)
	header_err_file.close()
	msg = """Error writing dicom header, see {0} for details."""
	try:
	    raise Error(msg.format(header_err_filepath))
	except Error, e:
	    print e.message
	    exit(1)
    else:
        f = open(header_filepath, 'w')
        f.write(stdout)
	f.close()

def send_email(jobids):
        # finally, we create a job that will wait till all the above jobs complete
        # before sending an email
        command = """#$ -hold_jid {jids}
#$ -M {user}@mail
#$ -m e""".format(jids=','.join(jobids), user=os.environ["USER"])


        script = create_qsub(command, "/dev/null")
        p = Popen(qsub, stdin=PIPE)
        p.communicate(script)

def main():
    options     = parse_options()
    
    tool        = options.tool
    root        = options.root
    destination = options.destination
    filetype    = options.filetype
    batch       = options.batch
    logdir	= options.logdir
    header	= options.header
    sync        = options.sync
    qsub        = ["qsub"]

    if not batch:
        root     = select_root()
        filetype = select_filetype()
    
    # convert to absolute path
    root = os.path.abspath(root)

    dcm_list = get_dicom_list(root)
    
    output_dir = destination
    
    jobids = []

    if header:
        check_dicom_hdr()

    for f in dcm_list:
        if destination is None:
            p = os.path.join(os.path.dirname(os.path.dirname(f)), "converted")
            output_dir = p
        
        output_dir = os.path.abspath(output_dir)
        
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        
        basename = os.path.basename(os.path.dirname(f)).lower()
        name = '.'.join([basename, filetype])
        
        output_filepath = os.path.join(output_dir, name)
        
        if (not sync or (sync and not os.path.exists(output_filepath))):
            command = "{tool} {input} {output}"
            
            if logdir is None:
                logdir = output_dir
            else:
                if not os.path.exists(logdir):
                    os.mkdir(logdir)
            
            script = create_qsub(command.format(
                                tool=tool,
                                input=f,
                                output=output_filepath), logdir)
                                
            p = Popen(qsub, stdin=PIPE, stdout=PIPE)
            output = p.communicate(script)
            
            if output[0] is not None:
                t = output[0].split()
                
                if len(t) >= 3 and t[2].isdigit():
                    jobids.append(t)
            
            if header:
                header_filepath = '.'.join([os.path.join(logdir, basename), "header"])
                write_header(f, header_filepath)

    if options.email:
        send_email(jobids)

if __name__ == "__main__":
    main()
