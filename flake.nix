{
  description = "A basic Nix Flake for eachDefaultSystem";

  inputs = {
    nixpkgs.url = "nixpkgs";
    utils.url = "github:numtide/flake-utils";

    polars-dev-flake.url = "github:coastalwhite/polars-dev-flake";
  };

  outputs = { self, nixpkgs, utils, polars-dev-flake }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in {
        devShells.default = polars-dev-flake.devShells.${system}.default;
      }
    );
}