# Virtual Machine for the labs

## Requirements

- VirtualBox
- Vagrant
- Ansible

## Setting up the VM

Install Ansible roles:

```
ansible-galaxy install -r requirements.yml -p roles
```

Build the VM:

```
vagrant up
```

Once done, reboot the VM to start the UI:

```
vagrant halt
vagrant up
```

Setup IntelliJ IDEA:

- Open a console
- Run `~/idea-*/bin/idea.sh`
- Configure IDEA:
  - Setup the Scala SDK
- Import project `/home/vagrant/coding-dojo-spark-ml/dojo-spark-ml`
- When required, setup the JDK from `/usr/lib/jvm/java-7-oracle`

Other things to configure:

- Add a shortcut to IntelliJ IDEA in the Launcher.
- Setup a keyboard (if not using an English keyboard).