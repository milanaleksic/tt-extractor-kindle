{
  description = "Extractor";
  inputs.flake-utils.url = "github:numtide/flake-utils";
  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system: {
        devShell =
          with nixpkgs.legacyPackages.${system};
          mkShell {
            buildInputs = [
                 curl
                 git
                 go_1_19
            ];
          };
      }
  );
}
