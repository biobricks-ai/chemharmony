{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.python39Full
    pkgs.python39Packages.pip
    pkgs.gcc
    pkgs.glibc
    pkgs.zlib  # Include zlib package
  ];

  shellHook = ''
    # Set library path for C++ standard library and zlib
    export LD_LIBRARY_PATH=${pkgs.lib.makeLibraryPath [ pkgs.stdenv.cc.cc pkgs.zlib ]}:$LD_LIBRARY_PATH

    # Create a Python virtual environment
    python -m venv .venv
    source .venv/bin/activate

    # Install dependencies from requirements.txt
    pip install -r requirements.txt
  '';
}
