Installation
=============

pip
----

The easiest is via pip::

    pip install pb-process-tools

Conda
------

You can also install from my conda channel (it is on my list to add to conda-forge channel)::

    conda install -c au-eoed -c conda-forge pb-process-tools

Docker
------

The docker images I create have the pbprocesstools module installed. For example::

    docker pull petebunting/au-eoed

You can run Python from this Docker image as shown below::

    docker run -itv $(PWD):/data petebunting/au-eoed python

Singularity
-------------

If you are working on a HPC system you won't have docker available but you can build a Singularity container from Docker::

    singularity build au-eoed.sif docker://petebunting/au-eoed