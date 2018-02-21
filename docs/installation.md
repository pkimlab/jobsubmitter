# Installation

## Stable release

To install Job Submitter, run this command in your terminal:

```bash
conda install jobsubmitter -c ostrokach
```

This is the preferred method to install Job Submitter, as it will always install the most recent stable release.

If you don't have [conda] installed, this [Python installation guide] can guide
you through the process.

[conda]: https://conda.io
[Python installation guide]: https://conda.io/docs/user-guide/install/index.html

## From sources

The sources for Job Submitter can be downloaded from the [GitLab repo].

You can either clone the public repository:

```bash
git clone git://gitlab.com/ostrokach/jobsubmitter
```

Or download the [tarball]:

```bash
curl -OL https://gitlab.com/ostrokach/jobsubmitter/repository/master/archive.tar
```

Once you have a copy of the source, you can install it with:

```bash
python setup.py install
```

[GitLab repo]: https://gitlab.com/ostrokach/jobsubmitter
[tarball]: https://gitlab.com/ostrokach/jobsubmitter/repository/master/archive.tar
