# Contributing

## Legal

Thank you for your interest in contributing to the `dbx` project (the “Project”). In order to clarify the intellectual property license granted with Contributions from any person or entity who contributes to the Project, Databricks, Inc. ("Databricks") must have a Contributor License Agreement (CLA) on file that has been signed by each such Contributor (or if an entity, an authorized representative of such entity). This license is for your protection as a Contributor as well as the protection of Databricks and its users; it does not change your rights to use your own Contributions for any other purpose.
You may sign this CLA either on your own behalf (with respect to any Contributions that are owned by you) and/or on behalf of an entity (the "Corporation") (with respect to any Contributions that are owned by such Corporation (e.g., those Contributions you make during the performance of your employment duties to the Corporation)).  Please mark the corresponding box below.
You accept and agree to the following terms and conditions for Your present and future Contributions submitted to Databricks. Except for the licenses granted herein to Databricks, You reserve all right, title, and interest in and to Your Contributions.
1. Definitions.
"You" (or "Your") shall mean the copyright owner or legal entity authorized by the copyright owner that is making this Agreement with Databricks. For legal entities, the entity making a Contribution and all other entities that control, are controlled by, or are under common control with that entity are considered to be a single Contributor. For the purposes of this definition, "control" means (i) the power, direct or indirect, to cause the direction or management of such entity, whether by contract or otherwise, or (ii) ownership of fifty percent (50%) or more of the outstanding shares, or (iii) beneficial ownership of such entity.
"Contribution" shall mean the code, documentation or any original work of authorship, including any modifications or additions to an existing work, that is submitted by You to Databricks for inclusion in, or documentation of, any of the products owned or managed by Databricks, including the Project, whether on, before or after the date You sign this CLA. For the purposes of this definition, "submitted" means any form of electronic, verbal, or written communication sent to Databricks or its representatives, including but not limited to communication on electronic mailing lists, source code control systems (e.g., Github), and issue tracking systems that are managed by, or on behalf of, Databricks for the purpose of discussing and improving the Project, but excluding communication that is conspicuously marked or otherwise designated in writing by You as "Not a Contribution."
2. Grant of Copyright License. Subject to the terms and conditions of this Agreement, You hereby grant to Databricks a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable copyright license to reproduce, prepare derivative works of, publicly display, publicly perform, sublicense (through multiple tiers), and distribute Your Contributions and such derivative works.  For the avoidance of doubt, and without limitation, this includes, at our option, the right to sublicense this license to recipients or users of any products or services (including software) distributed or otherwise made available (e.g., by SaaS offering) by Databricks (each, a “Downstream Recipient”).
3. Grant of Patent License. Subject to the terms and conditions of this Agreement, You hereby grant to Databricks a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable, sublicensable (through multiple tiers) (except as stated in this section) patent license to make, have made, use, offer to sell, sell, import, and otherwise transfer the Contribution in whole or in part, alone or in combination with any other products or services (including for the avoidance of doubt the Project), where such license applies only to those patent claims licensable by You that are necessarily infringed by Your Contribution(s) alone or by combination of Your Contribution(s) with the Project to which such Contribution(s) was submitted.  For the avoidance of doubt, and without limitation, this includes, at our option, the right to sublicense this license to Downstream Recipients.
4. Authorized Users. If you are signing this CLA on behalf of a Corporation, you may also add additional designated employees of the Corporation who will be covered by this CLA without the need to separately sign it (“Authorized Users”).  Your Primary Point of Contact (you or the individual specified below) may add additional Authorized Users at any time by contacting Databricks at cla@databricks.com (or such other method as Databricks informs you).
5. Representations. You represent that:
   1. You are legally entitled to grant the above licenses, and, if You are signing on behalf of a Corporation and have added any Authorized Users, You represent further that each employee of the Corporation designated by You is authorized to submit Contributions on behalf of the Corporation;
   2. each of Your Contributions is Your original creation;
   3. to your knowledge, Your Contributions do not infringe or otherwise misappropriate the intellectual property rights of a third person; and
   4. you will not assert any moral rights in your Contribution against us or any Downstream Recipients.
6. Support. You are not expected to provide support for Your Contributions, except to the extent You desire to provide support. You may provide support for free, for a fee, or not at all. Unless required by applicable law or agreed to in writing, and except as specified above, You provide Your Contributions on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, including, without limitation, any warranties or conditions of TITLE, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE.
7. Notification. It is your responsibility to notify Databricks when any change is required to the list of Authorized Users, or to the Corporation's Primary Point of Contact with Databricks.  You agree to notify Databricks of any facts or circumstances of which you become aware that would make the representations or warranties herein inaccurate in any respect.
8. This CLA is governed by the laws of the State of California and applicable U.S. Federal law. Any choice of law rules will not apply.

Please check one of the applicable statement below. Please do NOT mark both statements:
* ___ I am signing on behalf of myself as an individual and no other person or entity, including my employer, has or will have rights with respect my Contributions.
* ___ I am signing on behalf of my employer or a legal entity and I have the actual authority to contractually bind such entity (the Corporation).


| Name*:                                                                                                    |   |
|-----------------------------------------------------------------------------------------------------------|---|
| Corporation Entity Name (if applicable):                                                                  |   |
| Title or Role (if applicable):                                                                            |   |
| Mailing Address*:                                                                                         |   |
| Email*:                                                                                                   |   |
| Signature*:                                                                                               |   |
| Date*:                                                                                                    |   |
| Github Username (if applicable):                                                                          |   |
| Primary Point of Contact (if not you) (please provide name and email and Github username, if applicable): |   |
| Authorized Users (please list Github usernames):**                                                        |   |

- \* Required field
- ** Please note that Authorized Users may not be immediately be granted authorization to submit Contributions; should more than one individual attempt to sign a CLA on behalf of a Corporation, the first such CLA will apply and later CLAs will be deemed void.


## Nota bene about PR requirements

We happily welcome contributions to `dbx`.
We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.
Please create a PR only if you've created an issue related to it. PRs without a linked issue won't be reviewed.

## Local development

As a starting point, please install new environment as described in Makefile.
To make e2e runs you'll need a Databricks account, and a configured profile.

### Prerequisites
- `make`: This is the gnu make tool.
  - for mac: https://formulae.brew.sh/formula/make
- `pyenv`: https://github.com/pyenv/pyenv

Once you have the pre-requisites installed, you can run project functions like this:

```bash
make help
make clean install

make test
make test /tests/path/to/blah_test.py

make fix
make lint
```


## Pull Request Process

1. Please create a fork of this repository, and a development branch in it. You can name the branch as you would like to, but please make the branch name meaningful.
2. After finishing the development, please run the `make lint` to make sure that code is properly formatted.
3. When opening a PR, it's mandatory to reference an issue (or set of issues) it resolves. PRs without linked issues won't be reviewed.
4. Please describe the PR is 4-5 meaningful sentences. These sentences shall answer 3W questions (What is the problem, What is the impact, What is the solution)
5. Please add tests to your PR. PRs which decrease the coverage metric won't be resolved, unless it's a special case of a big refactoring.
6. If you add new functionality, please add some meaningful descriptions to the docs folder.
7. All commits shall have a GPG signature verification as per [this documentation](https://docs.github.com/en/github/authenticating-to-github/managing-commit-signature-verification/about-commit-signature-verification).

Please let us know if you've met a problem in the development setup via raising an issue. Happy coding!




