use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/refs/heads");

    let output = Command::new("git")
        .args(&["rev-parse", "HEAD"])
        .output()
        .expect("failed to get revision");
    let git_hash = String::from_utf8(output.stdout).unwrap();

    let git_diff = !Command::new("git")
        .args(&["diff-files", "--quiet"])
        .status()
        .expect("failed to check for changed files")
        .success();

    println!("cargo:rustc-env=GIT_HASH={}", &git_hash[..8]);
    println!("cargo:rustc-env=GIT_DIFF={}", u8::from(git_diff))
}
