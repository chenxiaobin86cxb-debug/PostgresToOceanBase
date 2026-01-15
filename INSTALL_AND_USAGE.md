# Package Installation and Usage

This package includes a prebuilt Python virtual environment under `venv/`
with all dependencies installed.

## Unpack

```bash
tar -xzf PostgresToOceanBase-package.tar.gz
cd PostgresToOceanBase
```

## Activate the Environment

Linux / macOS:

```bash
source venv/bin/activate
```

Windows:

```bat
venv\Scripts\activate
```

## Configure

Edit `config/config.yaml` to point to your PostgreSQL and OceanBase instances.
If you use environment variables for passwords, create a local `.env` file
in the project root (this file is not included in the package).

## Run

```bash
python src/main.py --config config/config.yaml --schema-only
python src/main.py --config config/config.yaml --data-only
python src/main.py --config config/config.yaml --validate
```

## Notes

- If the bundled `venv/` is not compatible with your OS or Python version,
  recreate it and reinstall dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
