# EA survey donations

This is for [Vipul Naik's donations website](https://github.com/vipulnaik/donations).
You will need the donations database from the donations repo to run the script
here.

The script here queries the donations database (specifically, the donees table)
to get the cause area for each donee, and will fail when a cause area is not
found. The idea is that there are relatively few donees in the EA Survey
responses, so that we can manually add them to the donees table as we encounter
new ones.

## License

CC0
