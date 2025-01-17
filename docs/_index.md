# Kubectl Kjob Plugin

This kubectl plugin allows ML researchers to run templated Jobs with different values
for the application-specific parameters, without the need to edit and submit entire
YAML files. The tool comes with built-in support for running slurm scripts inside
a Kubernetes cluster.

## Syntax

Use the following syntax to run `kubectl kjob` commands from your terminal window:

```shell
kubectl kjob [OPERATION] [TYPE] [NAME] [flags]
```

or with shorter syntax `kjobctl`:

```shell
kjobctl [OPERATION] [TYPE] [NAME] [flags]
```

You can go to the [commands](commands/_index.md) or run `kubectl kjob help` in the terminal to get the full list of commands, along with all possible flags.

## See Also

* [installation](installation.md)	 - Installation guide for the `kubectl-kjob` plugin, `kjobctl`.
* [commands](commands/kjobctl.md)	 - Full list of commands, along with all possible flags.