
# Intertidal

## Overview

This repository contains scripts needed to proof, store, and analyze
data used to generate intertidal clam and oyster harvest estimates for
public beaches in the Puget Sound basin. Methods used to calculate
harvest are defined in [Strom and
Bradbury, 2007](https://wdfw.wa.gov/publications/00944).

## Folder structure

The scripts are organized in a set of sequential folders to facilitate a
step-by-step workflow. Steps proceed as follows:

  - **Beach** (polygons of all beaches surveyed, for data entry)
  - **Creel** (catch per unit effort data from creel surveys)
  - **Flight** (counts of harvesters, includes ground-counts, and
    zero-counts)
  - **Seasons** (dates of open and closed seasons for managed beaches)
  - **Shares** (harvest shares, determined by State-Tribal agreements)

Each set of scripts typically includes notes in the header to document
any annual changes, issues, or special circumstances. Each folder also
includes a *data* subfolder to hold any needed raw data files. In the
future, once a front-end to the WDFW shellfish database has been
created, most of these scripts and subfolders will no longer be needed.
Raw data can then be entered directly using a web-interface, or via
automated procedures from mobile devices. Scripts to generate annual
harvest estimates and projections for the following year are stored at
the top level.

## Permissions

In order to use the scripts in this repository you **must** first be
granted permissions to the shellfish database. Database credentials are
defined using the internal .Renviron file. This avoids hard-coding
usernames or passwords anywhere in the scripts.
