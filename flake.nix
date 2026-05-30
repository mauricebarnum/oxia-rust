{
  description = "oxia-rust: experimental Rust Oxia client";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs?ref=nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
      crane,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        toolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rustfmt" "clippy" "llvm-tools" ];
        };
        craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;

        src = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter = path: type:
            (craneLib.filterCargoSources path type)
            || (pkgs.lib.hasSuffix ".proto" path)
            || (pkgs.lib.hasSuffix ".version" path)
            || (pkgs.lib.hasSuffix ".go" path)
            || (baseNameOf path == "go.mod")
            || (baseNameOf path == "go.sum");
          name = "source";
        };

        commonArgs = {
          inherit src;
          strictDeps = true;
          pname = "oxia-rust";
          version = "0.1.0";
          nativeBuildInputs = with pkgs; [
            go
            pkg-config
            protobuf
          ];
          buildInputs = with pkgs; [ ];
          PROTOC = "${pkgs.protobuf}/bin/protoc";
          PROTOC_INCLUDE = "${pkgs.protobuf}/include";
        };

        cargoArtifacts = craneLib.buildDepsOnly (commonArgs // { });
        oxia-rust = craneLib.buildPackage (commonArgs // { inherit cargoArtifacts; });
      in
      {
        devShells.default = craneLib.devShell {
          checks = self.checks.${system};

          packages = with pkgs; [
            go
            toolchain
            protobuf
            pkg-config
          ];

          PROTOC = "${pkgs.protobuf}/bin/protoc";
          PROTOC_INCLUDE = "${pkgs.protobuf}/include";
        };

        packages.default = oxia-rust;

        checks = {
          inherit oxia-rust;
          clippy = craneLib.cargoClippy (commonArgs // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets --all-features -- -D warnings";
          });
          fmt = craneLib.cargoFmt (commonArgs // { });
        };
      }
    );
}
