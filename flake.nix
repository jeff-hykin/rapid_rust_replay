{
  description = "Replay DimOS recordings onto LCM or Zenoh";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        # rusqlite builds a bundled SQLite, and zenoh links libc++/Security on
        # Darwin, so a C toolchain is needed on every platform.
        nativeBuildInputs = with pkgs; [ cargo rustc pkg-config ];
        buildInputs = with pkgs; [ openssl ]
          ++ lib.optionals stdenv.isDarwin [ libiconv ];
      in
      {
        packages.default = pkgs.rustPlatform.buildRustPackage {
          pname = "rapid_rust_replay";
          version = "0.1.0";
          # `target/` reaches many gigabytes during development and nix would
          # otherwise copy all of it into the store.
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter = path: type:
              !(type == "directory" && baseNameOf path == "target");
          };

          cargoLock = {
            lockFile = ./Cargo.lock;
            outputHashes = {
              "dimos-lcm-0.1.0" = "sha256-GGkx4Mn6NYP6KZecmoRLKGWIih/+y8OgNn12DeXX6n8=";
            };
          };

          inherit nativeBuildInputs buildInputs;

          meta = {
            description = "Replay DimOS recordings onto LCM or Zenoh";
            mainProgram = "rrr";
            license = pkgs.lib.licenses.asl20;
            platforms = pkgs.lib.platforms.unix;
          };
        };

        devShells.default = pkgs.mkShell {
          nativeBuildInputs = nativeBuildInputs ++ [ pkgs.rust-analyzer pkgs.clippy pkgs.rustfmt ];
          inherit buildInputs;
        };
      });
}
