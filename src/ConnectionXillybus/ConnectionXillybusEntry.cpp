/**
    @file ConnectionSTREAMEntry.cpp
    @author Lime Microsystems
    @brief Implementation of STREAM board connection.
*/
#ifdef __unix__
#include <unistd.h>
#endif
#include "ConnectionXillybusQSpark.h"
using namespace lime;
#include <fstream>


//! make a static-initialized entry in the registry
void __loadConnectionXillybusEntry(void) //TODO fixme replace with LoadLibrary/dlopen
{
    static ConnectionXillybusEntry XillybusEntry;
}


ConnectionXillybusEntry::ConnectionXillybusEntry(void):
    ConnectionRegistryEntry("PCIEXillybus")
{
}

ConnectionXillybusEntry::~ConnectionXillybusEntry(void)
{
}

std::vector<ConnectionHandle> ConnectionXillybusEntry::enumerate(const ConnectionHandle &hint)
{
    std::vector<ConnectionHandle> handles;
    ConnectionHandle handle;
#ifndef __unix__
    HANDLE fh = CreateFileA("\\\\.\\xillybus_control0_write_32", GENERIC_WRITE, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (fh != INVALID_HANDLE_VALUE || ::GetLastError() == ERROR_BUSY)
    {
        handle.media = "PCI-E";
        handle.name = "QSpark";
        handle.index = 1;
        handles.push_back(handle);
        CloseHandle(fh);
    }

    fh = CreateFileA("\\\\.\\xillybus_write_8", GENERIC_WRITE, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (fh != INVALID_HANDLE_VALUE || ::GetLastError() == ERROR_BUSY)
    {
        handle.media = "PCI-E";
        handle.name = "LimeSDR-PCIE";
        handle.index = 1;
        handles.push_back(handle);
        CloseHandle(fh);
    }

#else
    if( access("/dev/xillybus_control0_write_32", F_OK ) != -1 )
    {
        handle.media = "PCI-E";
        handle.name = "QSpark";
        handle.index = 1;
        handles.push_back(handle);
    }

    if( access("/dev/xillybus_write_8", F_OK ) != -1 )
    {
        handle.media = "PCI-E";
        handle.name = "LimeSDR-PCIE";
        handle.index = 1;
        handles.push_back(handle);
    }
#endif
    return handles;
}

IConnection *ConnectionXillybusEntry::make(const ConnectionHandle &handle)
{
    if (handle.name == "QSpark")
        return new ConnectionXillybusQSpark(handle.index);
    else
        return new ConnectionXillybus(handle.index);
}
