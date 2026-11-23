Gemini Skill for Code Generation
==================================

``pb_process_tools`` includes a Gemini Agent Skill in the repository under ``skills/pb-process-tools/`` so you can import accurate v4.x ``pb_process_tools`` knowledge (PostgreSQL job queues, ``PBPTQProcessTool``, ``PBPTGenQProcessToolCmds``, Slurm job arrays, ``pbpt_gen_template.py``, ``pbpt_clean_db.py``, and ``rsgispy`` integration) directly into Gemini for code generation.

What Is Included
----------------

The skill folder (``skills/pb-process-tools/``) contains:

* ``SKILL.md`` — Core code-generation rules, scaffolding commands, worker/generator templates, and the operational CLI lifecycle.
* ``references/api-reference.md`` — Exact signatures and parameter tables for ``PBPTQProcessTool``, ``PBPTGenQProcessToolCmds``, ``PBPTProcessToolsBase``, ``PBPTUtils``, and ``PBPTDatabaseCleaner``.
* ``examples/landsat_ndvi_workflow.md`` — End-to-end Landsat NDVI batch processing workflow using ``rsgispy`` and concurrent multi-task queue patterns.

Adding the Skill to Your Gemini Account Online
----------------------------------------------

You can upload the skill directly into your online Gemini account at `gemini.google.com <https://gemini.google.com>`_ (or Gemini Enterprise, where enabled by your administrator):

1. **Get the skill files**

   Clone the repository or download the ``skills/pb-process-tools/`` directory from GitHub (`https://github.com/remotesensinginfo/pb_process_tools <https://github.com/remotesensinginfo/pb_process_tools>`_)::

       git clone https://github.com/remotesensinginfo/pb_process_tools.git

   If you prefer to upload a single archive file, create a clean ``.zip`` with ``SKILL.md`` at the root of the archive (excluding hidden OS files such as ``.DS_Store``)::

       cd pb_process_tools/skills/pb-process-tools
       zip -r ../../pb-process-tools-skill.zip . -x "*.DS_Store" -x "__MACOSX/*"

2. **Open Skills in Gemini online**

   * Sign in to `https://gemini.google.com <https://gemini.google.com>`_.
   * In the left sidebar, click **Customize** (select **More** first if it is hidden).
   * Select **Skills** at the top, then click **Upload**.

3. **Upload and create the skill**

   * Select the ``skills/pb-process-tools`` folder, the ``pb-process-tools-skill.zip`` archive created above, or just ``SKILL.md``.
   * Verify the skill metadata (name ``pb-process-tools``) and click **Create**.

   .. note::
      When uploading a folder or ``.zip`` package to Gemini online, ``SKILL.md`` must sit at the root level of the uploaded folder/archive, and hidden files (such as ``.DS_Store``) should be omitted. Skills created on ``gemini.google.com`` do not automatically sync across separate Google Workspace or Chrome surfaces, so upload separately to each surface you use.

4. **Use and update the skill in chat**

   * Invoke the skill explicitly in any prompt by typing ``/pb-process-tools`` or ``@pb-process-tools``, or describe a ``pb_process_tools`` workflow task and let Gemini activate it automatically.
   * When a new version of ``pb_process_tools`` is released, update the skill via **More options -> Replace skill** in the Gemini **Skills** list.

Using the Skill in Gemini CLI / Local Projects
----------------------------------------------

If you also use Gemini CLI or local agent environments that follow the ``SKILL.md`` standard:

* **Install directly from GitHub via Gemini CLI**::

      # User-wide installation
      gemini skills install https://github.com/remotesensinginfo/pb_process_tools.git --path skills/pb-process-tools

      # Project/workspace-only installation
      gemini skills install https://github.com/remotesensinginfo/pb_process_tools.git --path skills/pb-process-tools --scope workspace

* **Or install manually by symlinking/copying**::

      # Global user skills directory
      mkdir -p ~/.gemini/skills
      ln -s /path/to/pb_process_tools/skills/pb-process-tools ~/.gemini/skills/pb-process-tools

      # Or project-local skills directory
      mkdir -p /path/to/your_project/.gemini/skills
      cp -r /path/to/pb_process_tools/skills/pb-process-tools /path/to/your_project/.gemini/skills/
