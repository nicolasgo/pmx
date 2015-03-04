# pmx

Installation
------------

Before running 'pmx', you need to install 'click' (http://http://click.pocoo.org/)

You can use the following command:

    pip install click

or install from the github repository https://github.com/mitsuhiko/click

    git clone https://github.com/mitsuhiko/click.git
    cd click
    python setup.py install


Install PMX

    git clone https://github.com/nicolasgo/pmx.git
    cd pmx 


Running PMX
-----------

List of pmx commands

    ./pmx

    Usage: pmx [OPTIONS] COMMAND [ARGS]...

        Pragmex analytic command line framework

    Options:
        --help  Show this message and exit.

    Commands:
        config   Display current configuration campaign
        current  Display current campaign
        edit     Edit current campaign configuration file
        list     Display all campaigns available
        new      Creates a new campaign.
        set      Set current campaign
        update   Update current campaign configuration


Creates a new campaign

    ./pmx new <name>


Edit current campaign configuration. This is normally followed by a 'pmx update' to activate the configuration changes
 

    ./pmx edit


Display the current campaign

    ./pmx current


Display the current campaign configuration

    ./pmx config


Display all campaigns available

    ./pmx list


Change or set current campaign

    ./pmx set <name>


Update current campaign configuration. After a 'pmx edit', it is mandatory after to do a 'pmx update' to activate the configuration changes.

    ./pmx update



Running campaigns
-----------------

After installing pmx, create a new campaign named 'pageview'

    ./pmx new pageview

    Campaign pageview created


    ./pmx edit

    <This will start your editor defined in EDITOR environment variable or 'vi' if EDITOR is not defined>
    <In your editor, add the filename to parse. Example 'test.log.a'>
    <input_files = ['test.log.a']>
    <save the file>


    ./pmx update

    Updating pageview campaign configuration


    ./clean.py

    <'clean.py' deletes all temporary files for this campaign>


    ./cat_pv

    files ['test.log.a']
    running dependencies... '.pv' files doesn't exist
    running dependencies... '.sort' files doesn't exist
    running dependencies... '.norm' files doesn't exist
    running dependencies... '.pre' files doesn't exist
    pre_fiter(ing) test.log.a
    normaliz(ing) test.log.a
    sort(ing) test.log.a
    pv(ing) test.log.a
    cat_pv(ing) test.log.a
   


Limitations (TO-DO)
-------------------

There's currently no 'pmx' python installation package available. After cloning the repository from github, you need to go directly to the installation directory to execute any commands.

    git clone https://github.com/nicolasgo/pmx.git
    cd pmx 

     
