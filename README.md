# hpc-ed-publishing-example-1

HPC-ED Publishing example 1 using the Globus SDK

This program is an example of how to publish and replace the HPC-ED metadata for a specific provider in an HPC-ED catalog. Given an input JSON file of all the learning material metadata for a provider, it loads that metadata into an HPC-ED catalog, and removes from the catalog previously published items from that provider that weren't in the input JSON file.

This program can be run by anyone that is authorized to write to the target catalog. The default  catalog is the *HPC Training Materials (HPC-ED) - Alpha Alpha v1*, which can be changed in a configuration file.

This program should work as is once configured appropriate. It can be forked/copied and then modified to load learning material metadata in a different format. See the *CUSTOMIZE HERE* section of the python program.

How to install and run this program:

1. Create a project directory (using any name you want):

    $ mkdir myproject
    $ cd myproject

2. Checkout this example

    $ git clone https://github.com/HPC-ED/hpc-ed-publishing-example-1.git

3. Make the PROD symlink point to the checkout

    $ ln -s hpc-ed-publishing-example-1 PROD

4. Create a Python3 with the required packages and activate it

    $ python3 -m venv mypython
    $ pip install -r PROD/requirements.txt
    $ pip install --upgrade pip
    $ source mypython/bin/activate

5. Directories needed by the example

    $ mkdir bin conf data var sbin

6. Copy the config template and configure

    $ cp PROD/conf/publishing-example-1.conf.template conf/publishing-example-1.conf

6.1. Customize the conf entries
    $ vi conf/publishing-example-1.conf
    -- Set the GLOBUS_CLIENT_ID
    -- Set the GLOBUS_CLIENT_SECRET
    -- Update the PROVIDER_ID replacing the trailing "unknown.org" with your own custom provider value
    -- Optionally, change the INDEX_ID to any other HPC-ED Catalog you have access to
    
7. Run the example

    $ python3 ./PROD/bin/publishing-example-1.py -c conf/publishing-example-1.conf -s file:PROD/data/publishing-example-1-sample1.json -l debug

8. Look at the logs

    $ tail -50 var/publishing-example-1.log

9. Try customizing things

    -- Create your own alternate sample json data in your data/ directory
    -- Copy the program from PROD/bin to bin/ and customize it to work with alternate input JSON formats
    
10. Tell us what you think

    -- If you run into any problems running the example or want to suggest improvements, please post them as GitHub issues


NOTES:

-- The above deployment approach keeps the GitHub checkout and your customizations separate
