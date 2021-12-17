# cloudfree-hextiles-client
Hextiles Client

Will download tiles starting from a given Level 6 h3 hex tile id, out to a specified radius distance away.


## Usage

### Setup

```
# Install libraries
pipenv sync --dev
pipenv shell

# Make download dir
mkdir tiles
cd tiles
```

### Download

```
# Download some tiles!
> python ../download_tiles.py --help

Usage: download_tiles.py [OPTIONS]

Options:
  --start-id TEXT                 [default: 861972387ffffff]
  --distance INTEGER              [default: 1]
  --year INTEGER                  [default: 2020]
  --month INTEGER                 [default: 7]
  --verbose / --no-verbose        [default: no-verbose]
  --install-completion [bash|zsh|fish|powershell|pwsh]
                                  Install completion for the specified shell.
  --show-completion [bash|zsh|fish|powershell|pwsh]
                                  Show completion for the specified shell, to
                                  copy it or customize the installation.
  --help                          Show this message and exit.
```

### Example

```
# Example:
python ../download_tiles.py --start-id 86190d3afffffff --distance 4
